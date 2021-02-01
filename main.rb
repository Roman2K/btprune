require 'utils'
require 'time'

class App
  def initialize(client, pvrs: [], dry_run: true, log:)
    @client, @pvrs = client, pvrs
    @dry_run = dry_run
    @log = log
    @log[dry_run: @dry_run].debug "initialized"
  end

  def cmd_prune
    categories = {
      "uncat_ok" => :imported,
      "" => nil,
    }
    @pvrs.each do |pvr|
      categories[pvr.name.downcase] = pvr
    end

    cleaner = Cleaner.new @client, categories, dry_run: @dry_run, log: @log
    cleaner.clean
    cleaner.prevent_quota_overage

    @log.info "freed %s by deleting %d torrents" \
      % [Utils::Fmt.size(cleaner.freed), cleaner.deleted_count]
    @log.info "marked %d torrents as failed" % [cleaner.failed_count]
  end
end

class Torrent < BasicObject
  def initialize(t); @t = t end
  private def method_missing(m,*a,&b); @t.public_send m,*a,&b end
  attr_accessor :log, :status, :pvr
end

class Cleaner
  def initialize(client, categories, dry_run:, log:)
    @client = client
    @categories = categories
    @dry_run = dry_run
    @log = log

    @queues = Hash.new { |cache, pvr| cache[pvr] = pvr.queue }
    @freed = @deleted_count = @failed_count = 0
  end

  attr_reader :freed, :deleted_count, :failed_count

  private def real_mode
    yield unless @dry_run
  end

  MAX_QUOTA = 200 * (1024 ** 3)

  def prevent_quota_overage
    torrents = @client.torrents
    free = MAX_QUOTA
    r = Resumes.new
    torrents.sort_by { |t|
      [ case
        when t.progress >= 0 then 0
        when t.downloading? then 1
        else 2
        end,
        -t.progress ]
    }.each do |t|
      free -= t.size
      next unless t.progress < 1
      if free < 0
        @log[
          t: t.name,
          overage: "%s over %s" % [-free, MAX_QUOTA].map { Utils::Fmt.size _1 },
        ].debug "should pause to prevent quota overage"
        r.pause
      else
        r.resume
      end << t
    end
    r.optimize!

    @log.info "pausing #{r.pause.size} torrents" do
      real_mode { @client.pause r.pause }
    end
    @log.info "resuming #{r.resume.size} torrents" do
      real_mode { @client.resume r.resume }
    end
  end

  def clean
    torrents = @client.torrents.map do |t|
      t = Torrent.new t
      cat = t.cat
      cat = "[no category]" if cat.empty?
      t.log = @log[cat, torrent: t.name]
      t
    end
    assign_statuses! torrents

    init_used = torrents.sum { |t| t.size * t.progress }
    @log.info "used: %s of %s" \
      % [init_used, MAX_QUOTA].map { Utils::Fmt.size _1 }

    prev_used = nil
    is_over_max = ->{ (init_used - @freed) >= MAX_QUOTA }
    torrents.
      map { |t| [t, SeedStats.new(t)] }.
      sort_by { |t, st| -st.ratio }.
      each { |t, st|
        may_delete t, st, should_free: is_over_max.()
        if (used = init_used - @freed) != prev_used
          @log.info "used after deletes: #{Utils::Fmt.size used}"
          prev_used = used
        end
      }

    @log.error "failed to free enough disk space" if is_over_max.()
  end

  private def assign_statuses!(tors)
    tors = tors.group_by(&:cat)
    @categories.each do |cat, pvr|
      ts = tors.delete(cat) { [] }
      ts.each { |t| t.pvr = pvr } if Utils::PVR::Basic === pvr
      assign_statuses_from_pvr! ts, pvr
    end
    tors.each do |_, ts|
      ts.each { |t| t.status = :unknown_cat }
    end
  end

  private def assign_statuses_from_pvr!(tors, pvr)
    if !pvr
      tors.each { |t| t.status = :no_pvr }
      return
    end
    if pvr == :imported
      tors.each { |t| t.status = pvr }
      return
    end

    tors = tors.each_with_object({}) { |t,h| h[t.hash_string.downcase] = t }
    statuses = {}
    events = pvr.history_events

    until tors.empty?
      ev = begin
        events.next
      rescue StopIteration
        break
      end
      ev = Utils::PVR::Event.of_pvr(pvr, ev)
      ev.group_key.then do |key|
        date = Time.parse(ev.fetch("date"))
        info = statuses[key]
        source_title = ev.fetch "sourceTitle"
        st =
          case ev.fetch("eventType")
          when "downloadFolderImported" then :imported
          when "grabbed" then :grabbed
          end
        if !info || (
          info.fetch(:date) < date \
            && (source_title == info.fetch(:source_title) || st == :grabbed)
        ) then
          info = statuses[key] = {
            date: date,
            status: st,
            source_title: source_title,
          }
        end
        info
      end
      # Torrent hash may be nil while loading metadata
      t = (cl = ev.fetch("data")["downloadClient"]&.downcase \
        and %w[qbittorrent qbt transmission].include?(cl.downcase) \
        and id = ev["downloadId"] \
        and tors.delete(id.downcase)) or next
      t.status = statuses.values_at(*ev.group_keys).compact.
        sort_by { _1.fetch(:date) }.
        fetch(-1).fetch :status
    end
  end

  def may_delete(t, st, should_free:)
    log = t.log[status: t.status]

    case t.status
    when :unknown_cat
      log.error "unknown category"
      return
    when :no_pvr
      log.debug "no configured PVR"
      return
    end

    log = log[progress: Fmt.progress(st.progress), ratio: Fmt.ratio(st.ratio)]

    dl_log = -> { log[
      torrent_state: t.state,
      time_active: Fmt.duration(st.time_active),
      health: Fmt.score(st.health),
    ] }

    if !st.health.ok
      dl_log.().info "low health, marking as failed" do
        mark_failed t, log: log
      end
      return
    end

    if st.progress < 1
      dl_log.().debug "still downloading"
      return
    end

    log = log[
      seed_time: st.seed_time.then { |t| t ? Fmt.duration(t) : "?" },
      seed_score: Fmt.score(st.seeding),
    ]

    if !st.seeding.ok
      log.debug "still seeding"
      return
    end

    should_free or return
    log.debug "should free up space"

    case t.status
    when :imported
      log.info("imported, deleting") { delete t }
    else
      log.error "unhandled status, not deleting"
    end
  end

  private def delete(t)
    real_mode { @client.delete_perm [t] }
    @deleted_count += 1
    @freed += t.size
  end

  private def mark_failed(t, log:)
    qid = @queues[t.pvr].find { |item|
      id = item["downloadId"] and id.downcase == t.hash_string.downcase
    }&.fetch "id"

    unless qid
      log[torrent_state: t.state].
        warn "item not found in PVR queue, deleting torrent" do
          delete t
        end
      return
    end

    log[queue_item: qid].debug "deleting from queue and blacklisting" do
      real_mode { t.pvr.queue_del qid, blacklist: true }
      @failed_count += 1
    end
  end

  module Fmt
    def self.duration(*args, &block)
      Utils::Fmt.duration *args, &block
    end
    
    def self.ratio(r)
      Utils::Fmt.d r, 2, z: false
    end

    def self.progress(f)
      Utils::Fmt.pct f, 1, z: false
    end

    def self.score(s)
      "%s:%s" % [progress(s.to_f), s.ok ? "OK" : "!!"]
    end
  end
end

class Resumes
  def initialize
    @pause, @resume = [], []
  end

  attr_reader :pause, :resume

  def optimize!
    @resume, @pause[0,1] = @pause[0,1], [] if @resume.empty?
    backup = @pause.select { _1.state != 'stalledDL' }.shuffle
    @resume.each_with_index do |t, idx|
      if t.state == 'stalledDL' && o = backup.shift
        @resume[idx] = o
        @pause.delete(o) or raise
        @pause << t
      end
    end
    @resume.reject! &:downloading?
    @pause.select! &:downloading?
    self
  end
end

class SeedStats
  def initialize(t)
    @state = t.state
    @progress = t.progress
    @ratio = [t.ratio, 0].max

    now = Time.now
    @time_active = t.time_active
    @seed_time = (now - Time.at(t.completion_on) if @progress >= 1)

    @health = Score.new compute_health_score
    @seeding = Score.new \
      self.class.compute_seeding_score(@ratio, @seed_time, t.size)
  end

  MIN_SEED_RATIO = 5
  MIN_SEED_MAX_SIZE = 15 * 1024**3
  SEED_TIME_LIMIT = 2 * 86400

  def self.compute_seeding_score(ratio, seed_time, size)
    target = ((MIN_SEED_MAX_SIZE * MIN_SEED_RATIO).to_f / size).clamp(1, 10)
    scores = [ratio.to_f / target]
    scores << (seed_time.to_f / SEED_TIME_LIMIT) if seed_time
    scores.max
  end

  DL_TIME_LIMIT = 1 * 86400
  DL_GRACE = 2 * 3600

  private def compute_health_score
    unless @progress < 1 && %w[stalledDL metaDL].include?(@state)
      return 1
    end
    [2 - [@time_active - DL_GRACE, 0].max / DL_TIME_LIMIT + @progress, 0].max
  end

  attr_reader \
    :progress, :ratio,
    :time_active, :seed_time, :health, :seeding

  Score = Struct.new :num do
    def to_f; num.to_f end
    def ok; num >= 1 end
  end
end

if $0 == __FILE__
  require 'metacli'
  config = Utils::Conf.new "config.yml"
  log = Utils::Log.new $stderr, level: :info
  log.level = :debug if ENV["DEBUG"] == "1"
  dry_run = config[:dry_run]
  client = Utils::QBitTorrent.new URI(config[:qbt]), log: log["qbt"]
  pvrs = config[:pvrs].to_hash.map do |name, url|
    Utils::PVR.const_get(name).new(URI(url), batch_size: 100, log: log[name])
  end
  app = App.new client, pvrs: pvrs, dry_run: dry_run, log: log
  MetaCLI.new(ARGV).run app
end
