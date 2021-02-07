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

    @log.info "freed %s by deleting %d torrents" \
      % [Utils::Fmt.size(cleaner.freed), cleaner.deleted_count]
    @log.info "marked %d torrents as failed" % [cleaner.failed_count]
  end
end

class Torrent < BasicObject
  def initialize(t); @t = t end
  private def method_missing(m,*a,&b); @t.public_send m,*a,&b end
  attr_accessor :log, :status, :pvr, :download_client_id
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

  private def real_mode(ret=nil)
    @dry_run ? ret : yield
  end

  MAX_QUOTA = 200 * (1024 ** 3)

  private def prevent_quota_overage(torrents)
    log = @log["quota"]
    torrents = torrents.select &:pvr
    free = MAX_QUOTA - torrents.sum { |t| t.size * t.progress }
    if free >= 0
      [:info, "remaining: %s of %s", free]
    else
      [:warn, "overage: %s over %s", -free]
    end.then do |lvl, msg, sz| 
      log.public_send lvl, msg % [sz, MAX_QUOTA].map { Utils::Fmt.size _1 }
    end
    dling = torrents.select { _1.progress < 1 }
    r = Resumes.new(torrents.size - dling.size > 0 ? 0 : 1)
    dling.sort_by { -_1.progress }.each do |t|
      rem = t.size * (1 - t.progress)
      free -= rem
      if free < 0
        log[
          t: t.name,
          rem: Utils::Fmt.size(rem),
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
    torrents = @client.torrents.map { |t|
      t = Torrent.new t
      cat = t.cat
      cat = "[no category]" if cat.empty?
      t.log = @log[cat, torrent: t.name]
      t
    }.tap { assign_statuses! _1 }

    init_used = torrents.sum { |t| t.size * t.progress }
    @log.info "used: %s of %s" \
      % [init_used, MAX_QUOTA].map { Utils::Fmt.size _1 }

    prev_used = nil
    is_over_max = ->{ (init_used - @freed) >= MAX_QUOTA }
    torrents.
      map { |t| [t, SeedStats.new(t)] }.
      sort_by { |t, st| -st.ratio }.
      each { |t, st|
        ok = may_delete t, st, should_free: is_over_max.()
        if (used = init_used - @freed) != prev_used
          @log.info "used after deletes: #{Utils::Fmt.size used}"
          prev_used = used
        end
        torrents.delete t or raise if ok
      }

    @log.error "failed to free enough disk space" if is_over_max.()

    force_imports torrents
    prevent_quota_overage torrents
  end

  IMPORT_ROOT_MAP_FROM = Pathname "/downloads"
  IMPORT_ROOT_MAP_TO = Pathname "/torrents"

  private def force_imports(torrents)
    torrents = torrents.select { |t| t.progress >= 1 && t.status == :grabbed }
    return if torrents.empty?
    running = Hash.new do |h, pvr|
      h[pvr] = pvr.commands.
        select { |c| c.fetch("name") == pvr.class::CMD_DOWNLOADED_SCAN }.
        sort_by { |c| c.fetch("started") }
    end
    torrents.each do |t|
      log = t.log["import"]
      path = Pathname(t.path).relative_path_from(IMPORT_ROOT_MAP_FROM).
        tap { _1.descend.first.to_s != '..' or raise "unexpected root" }.
        then { IMPORT_ROOT_MAP_TO.join _1 }
      log[path: path].info "should force import"
      if cmd = running[t.pvr].
        find { _1.fetch("body").fetch("path") == path.to_s }
      then
        log[id: cmd.fetch("id")].info "found running command"
        next
      end
      cmd = real_mode "id" => "[dry run]" do
        t.pvr.downloaded_scan path.to_s,
          download_client_id: t.download_client_id, import_mode: :copy
      end
      log[id: cmd.fetch("id"), download_client_id: t.download_client_id].
        info "started command"
    end
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
        info = statuses[key]
        new_info = {
          date: Time.parse(ev.fetch("date")),
          source_title: ev.fetch("sourceTitle"),
          status: \
            case ev.fetch("eventType")
            when "downloadFolderImported" then :imported
            when "grabbed" then :grabbed
            when "downloadFailed" then :dl_failed
            else :unknown
            end,
        }
        info = nil if info && newer_status_info?(new_info, info)
        info || (statuses[key] = new_info)
      end
      # Torrent hash may be nil while loading metadata
      t = (cl = ev.fetch("data")["downloadClient"]&.downcase \
        and %w[qbittorrent qbt transmission].include?(cl.downcase) \
        and id = ev["downloadId"] \
        and tors.delete(id.downcase)) or next
      t.download_client_id = id
      t.status = statuses.values_at(*ev.group_keys).compact.
        sort_by { _1.fetch(:date) }.
        fetch(-1).fetch :status
    end
  end

  private def newer_status_info?(a,b)
    more_recent = a.fetch(:date) > b.fetch(:date)
    if a.fetch(:source_title) == b.fetch(:source_title)
      more_recent
    elsif more_recent
      a.fetch(:status) == :grabbed
    else
      a.fetch(:status) == :imported && b.fetch(:status) != :grabbed
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

    if err = queue_fatal_err(t)
      dl_log.()[err: err].info "queue error, marking as failed" do
        mark_failed t, log: log
      end
      return true
    end

    if !st.health.ok
      dl_log.().info "low health, marking as failed" do
        mark_failed t, log: log
      end
      return true
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
      log[seeding_info: st.seeding_info].debug "still seeding"
      return
    end

    should_free or return
    log.debug "should free up space"

    case t.status
    when :imported
      log.info("imported, deleting") { delete t }
      return true
    else
      log.error "unhandled status, not deleting"
    end

    false
  end

  private def delete(t)
    real_mode { @client.delete_perm [t] }
    @deleted_count += 1
    @freed += t.size
  end

  private def queue_fatal_err(t)
    qit = queue_item(t) or return
    qit.fetch("statusMessages").each do
      msgs = _1.fetch "messages"
      if msgs.any? /Unable to parse file/i
        return msgs.join ", "
      end
    end
    nil
  end

  private def queue_item(t)
    @queues[t.pvr].find do |item|
      id = item["downloadId"] and id.downcase == t.hash_string.downcase
    end
  end

  private def mark_failed(t, log:)
    unless qid = queue_item(t)&.fetch("id")
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
  def initialize(min_dl)
    @min_dl = min_dl
    @pause, @resume = [], []
  end

  attr_reader :pause, :resume

  STATE_STALLED = 'stalledDL'

  def optimize!
    @resume, @pause[0,@min_dl] = @pause[0,@min_dl], [] if @resume.empty?
    backup = @pause.select { _1.state != STATE_STALLED }.shuffle
    @resume.each_with_index do |t, idx|
      if t.state == STATE_STALLED && o = backup.shift
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
    compute_seeding_score(t.size).then do |score, seeding_info|
      @seeding = Score.new score
      @seeding_info = seeding_info
    end
  end

  attr_reader \
    :progress, :ratio,
    :time_active, :seed_time, :health, :seeding, :seeding_info

  DL_TIME_LIMIT = 1 * 86400
  DL_GRACE = 2 * 3600

  private def compute_health_score
    unless @progress < 1 && %w[stalledDL metaDL].include?(@state)
      return 1
    end
    [2 - [@time_active - DL_GRACE, 0].max / DL_TIME_LIMIT + @progress, 0].max
  end

  MIN_SEED_RATIO = 5
  MIN_SEED_MAX_SIZE = 15 * 1024**3
  SEED_TIME_LIMIT = 2 * 86400

  private def compute_seeding_score(size)
    target = MIN_SEED_RATIO
    target *= [MIN_SEED_MAX_SIZE.to_f / size, 1].min
    target = 1 if target < 1
    scores = [@ratio.to_f / target]
    scores << (@seed_time.to_f / SEED_TIME_LIMIT) if @seed_time
    [scores.max, target_ratio: target]
  end

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
