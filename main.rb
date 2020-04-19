require 'utils'

class App
  def initialize(qbt, radarr: nil, sonarr: nil, dry_run: true, log:)
    @qbt, @radarr, @sonarr = qbt, radarr, sonarr
    @dry_run = dry_run
    @log = log
    @log[dry_run: @dry_run].debug "initialized"
  end

  def cmd_prune
    qbt = begin
      Utils::QBitTorrent.new @qbt, log: @log["qbt"]
    rescue => err
      Utils.is_unavail?(err) or raise
      @log[err: err].debug "qBitTorrent HTTP API seems unavailable, aborting"
      exit 0
    end

    imported = {
      "radarr" => @radarr,
      "sonarr" => @sonarr,
      "lidarr" => nil,
      "uncat_ok" => :done,
      "" => nil,
    }.tap { |h|
      h.each do |cat, uri|
        h[cat] =
          case uri
          when :done then uri
          else
            begin
              send "imported_#{cat}", uri if uri
            rescue => err
              raise unless Utils.is_unavail?(err)
              @log[err: err].warn "#{cat} HTTP API seems unavailable, aborting"
              nil
            end
          end
      end
    }

    cleaner = Cleaner.new qbt, imported, dry_run: @dry_run
    qbt.torrents.each { |t| cleaner.clean t, log: @log }
    @log.info "freed %s by deleting %d torrents" \
      % [Utils::Fmt.size(cleaner.freed), cleaner.deleted_count]
    @log.info "marked %d torrents as failed" % [cleaner.failed_count]
  end

  private def imported_radarr(uri)
    imported_pvr Utils::PVR::Radarr.new(uri, log: @log["radarr"]) do |ev|
      ev.fetch "movieId"
    end
  end

  private def imported_sonarr(uri)
    imported_pvr Utils::PVR::Sonarr.new(uri, log: @log["sonarr"]) do |ev|
      %w( seriesId episodeId ).map { |k| ev.fetch k }
    end
  end

  private def imported_pvr(pvr)
    done = imported_from_history(pvr.history) { |ev| yield ev }
    [done, pvr]
  end

  private def imported_from_history(hist)
    done = {}
    by_id = {}

    hist.
      sort_by { |ev| ev.fetch "date" }.
      each { |ev|
        hash = (cl = ev.fetch("data")["downloadClient"] \
          and %w[qbittorrent qbt].include?(cl.downcase) \
          and ev.fetch("downloadId").downcase) or next

        id = done[hash] = yield ev
        date = ev.fetch "date"
        update = -> ok do
          st = by_id[id]
          next if st && st.date > date
          by_id[id] = ImportStatus.new(date, ok)
        end

        case ev.fetch("eventType")
        when "grabbed" then update[false]
        when "downloadFolderImported" then update[true]
        end
      }

    # Before: hash -> id
    # After: hash -> ok
    done.transform_values do |id|
      by_id.fetch(id).ok
    end
  end

  ImportStatus = Struct.new :date, :ok
end

class Cleaner
  def initialize(qbt, imported, dry_run:)
    @qbt = qbt
    @imported = imported
    @dry_run = dry_run
    @queues = Hash.new { |cache, pvr| cache[pvr] = pvr.queue }
    @freed = @deleted_count = @failed_count = 0
  end

  attr_reader :freed, :deleted_count, :failed_count

  def clean(t, log:)
    cat = t.fetch "category"
    log = log[
      "in %s" % cat.yield_self { |s| s.empty? ? "[none]" : s },
      torrent: t.fetch("name")
    ]
    cat_done, pvr = @imported.fetch cat do
      log.error "unknown category"
      return
    end
    if !cat_done
      log.debug "no configured PVR"
      return
    end

    st = SeedStats.new t
    log = log[
      progress: Fmt.progress(st.progress),
      ratio: "%s of %s" % [Fmt.ratio(st.ratio), Fmt.ratio(st.min_ratio)],
    ]

    health = st.health
    if !health.ok
      log[
        added_time: Fmt.duration(st.added_time),
        avail: Fmt.ratio(st.availability),
        health: Fmt.score(health),
      ].info "low health, marking as failed" do
        mark_failed pvr, t, log: log
      end
      return
    end

    if st.progress < 1
      log.debug "still downloading"
      return
    end

    seeding = st.seeding
    log = log[
      seed_time: "%s of %s" \
        % [st.seed_time, st.time_limit].map { |t| t ? Fmt.duration(t) : "?" },
      seed_score: Fmt.score(seeding),
    ]

    if !seeding.ok
      log.debug "still seeding"
      return
    end

    import_ok =
      case cat_done
      when :done then true
      else
        cat_done.fetch t.fetch("hash").downcase do
          log.warn "not found in PVR"
          return
        end
      end
    if !import_ok
      log.warn "not imported by PVR"
      return
    end

    log.info "fully seeded, deleting" do
      real_mode { @qbt.delete_perm t }
      @deleted_count += 1
      @freed += t.fetch("size")
    end
  end

  private def mark_failed(pvr, t, log:)
    qid = @queues[pvr].find { |item|
      item.fetch("downloadId").downcase == t.fetch("hash").downcase
    }&.fetch "id"

    unless qid
      log[torrent_state: t.fetch("state")].
        warn "item not found in PVR queue, deleting torrent" do
          real_mode { @qbt.delete_perm t }
        end
      return
    end

    log[queue_item: qid].debug "deleting from queue and blacklisting" do
      real_mode { pvr.queue_del qid, blacklist: true }
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
      "%s (%s)" % [progress(s.to_f), s.ok ? "OK" : "!!"]
    end
  end
end

class SeedStats
  DEFAULT_MIN_RATIO = 10
  DEFAULT_TIME_LIMIT = 30 * 24 * 3600
  DEFAULT_DL_TIME_LIMIT = 30 * 24 * 3600
  DEFAULT_DL_GRACE = 1 * 24 * 3600

  def initialize(t,
    dl_time_limit: DEFAULT_DL_TIME_LIMIT, dl_grace: DEFAULT_DL_GRACE
  )
    @dl_time_limit = dl_time_limit
    @dl_grace = dl_grace

    @state = t.fetch "state"
    @progress = t.fetch "progress"
    @ratio = t.fetch "ratio"
    @min_ratio = t.fetch("max_ratio").
      yield_self { |r| r > 0 ? r : DEFAULT_MIN_RATIO }

    now = Time.now
    @added_time = now - Time.at(t.fetch "added_on")
    @availability = t.fetch "availability"

    @seed_time = (now - Time.at(t.fetch "completion_on") if @progress >= 1)
    @time_limit = t.fetch("max_seeding_time").
      yield_self { |mins| mins > 0 ? mins * 60 : DEFAULT_TIME_LIMIT }

    @health = Score.new compute_health_score
    @seeding = Score.new compute_seeding_score
  end

  private def compute_seeding_score
    @seed_time or return 0
    @ratio.to_f / @min_ratio \
      + @seed_time.to_f / @time_limit
  end

  private def compute_health_score
    @progress < 1 or return 1
    @state != Statuses::ERROR or return 0
    @state == Statuses::STALLED_DL or return 1
    [1 - (@added_time - @dl_grace) / @dl_time_limit, 0].max \
      + @progress * (@availability * 2)
  end

  attr_reader \
    :progress,
    :ratio, :min_ratio,
    :seed_time, :time_limit, :seeding,
    :added_time, :availability, :health

  Score = Struct.new :num do
    def to_f; num.to_f end
    def ok; num >= 1 end
  end

  module Statuses
    DOWNLOADING = "downloading".freeze
    ERROR = "error".freeze
    STALLED_DL = "stalledDL".freeze
  end
end

if $0 == __FILE__
  require 'metacli'

  config = Utils::Conf.new "config.yml"
  dry_run = config["dry_run"]
  qbt = URI config["qbt"]
  pvrs = %i( radarr sonarr ).each_with_object({}) do |name, h|
    url = config[name] or next
    h[name] = URI url
  end

  log = Utils::Log.new $stderr, level: :info
  log.level = :debug if ENV["DEBUG"] == "1"

  app = App.new qbt, dry_run: dry_run, log: log, **pvrs
  MetaCLI.new(ARGV).run app
end
