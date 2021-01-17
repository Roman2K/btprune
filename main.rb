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
      "lidarr" => nil,
      "uncat_ok" => :imported,
      "" => nil,
    }
    @pvrs.each do |pvr|
      categories[pvr.name.downcase] = pvr
    end

    cleaner = Cleaner.new @client, categories, dry_run: @dry_run, log: @log
    cleaner.clean

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

  MAX_DISK_USE = 200 * (1024 ** 3)

  def clean
    torrents = @client.torrents.map do |t|
      t = Torrent.new t
      cat = t.cat
      cat = "[no category]" if cat.empty?
      t.log = @log[cat, torrent: t.name]
      t
    end
    assign_statuses! torrents

    init_used = torrents.sum &:size
    @log.info "used: %s of %s" \
      % [init_used, MAX_DISK_USE].map { Utils::Fmt.size _1 }

    prev_used = nil
    is_over_max = ->{ (init_used - @freed) >= MAX_DISK_USE }
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

    tors = tors.each_with_object({}) do |t,h|
      h[t.hash_string.downcase] = t
    end

    pvr.history_events.each do |ev|
      # Torrent hash may be nil while loading metadata
      t = (cl = ev.fetch("data")["downloadClient"]&.downcase \
        and %w[qbittorrent qbt transmission].include?(cl.downcase) \
        and id = ev["downloadId"] \
        and tors.delete(id.downcase)) or next
      t.status =
        case ev.fetch("eventType")
        when "grabbed" then :grabbed
        when "downloadFolderImported" then :imported
        end
      break if tors.empty?
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

    health = st.health
    dl_log = -> { log[
      torrent_state: t.state,
      added_time: Fmt.duration(st.added_time),
      health: Fmt.score(health),
    ] }

    if !health.ok
      dl_log.().info "low health, marking as failed" do
        mark_failed t, log: log
      end
      return
    end

    if st.progress < 1
      dl_log.().info "still downloading"
      return
    end

    seeding = st.seeding
    log = log[
      seed_time: st.seed_time.then { |t| t ? Fmt.duration(t) : "?" },
      seed_score: Fmt.score(seeding),
    ]

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
      id = item["downloadId"] \
        and id.downcase == t.hash_string.downcase
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

  private def real_mode
    yield unless @dry_run
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

class SeedStats
  DEFAULT_DL_TIME_LIMIT = 4 * 24 * 3600
  DEFAULT_DL_GRACE = 1 * 3600

  def initialize(t)
    @dl_time_limit = DEFAULT_DL_TIME_LIMIT
    @dl_grace = DEFAULT_DL_GRACE

    @state = t.state
    @progress = t.progress
    @ratio = [t.ratio, 0].max

    now = Time.now
    @added_time = now - Time.at(t.added_on)
    @seed_time = (now - Time.at(t.completion_on) if @progress >= 1)

    @health = Score.new compute_health_score
    @seeding = Score.new @ratio
  end

  private def compute_health_score
    unless @progress < 1 && %w[stalledDL metaDL].include?(@state)
      return 1
    end
    [1 - (@added_time - @dl_grace) / @dl_time_limit, 0].max + @progress * 2
  end

  attr_reader \
    :progress, :ratio,
    :added_time, :seed_time, :health, :seeding

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
