require 'timeout'
require 'utils'

class App
  def initialize(log)
    @log = log
  end

  CONN_TIMEOUT = 20

  def cmd_prune(qbt, radarr: nil, sonarr: nil)
    qbt = begin
      Utils.try_conn! CONN_TIMEOUT do
        Utils::QBitTorrent.new URI(qbt), log: @log["qbt"]
      end
    rescue Utils::ConnError
      @log[err: $!].debug "qBitTorrent HTTP API seems unavailable, aborting"
      exit 0
    end

    done = {
      "radarr" => radarr,
      "sonarr" => sonarr,
      "lidarr" => nil,
      "uncat_ok" => :done,
      "" => nil,
    }.tap { |h|
      h.each do |cat, url|
        h[cat] =
          case url
          when :done then url
          else
            begin
              send "#{cat}_done", URI(url) if url
            rescue Utils::ConnError
              @log[err: $!].warn "#{cat} HTTP API seems unavailable, aborting"
            end
          end
      end
    }

    qbt.completed.each do |t|
      auto_delete qbt, t, done
    end
  end

  private def auto_delete(qbt, t, done)
    cat = t.fetch "category"
    log = @log[
      "in %s" % cat.yield_self { |s| s.empty? ? "[none]" : s },
      torrent: t.fetch("name")
    ]
    cat_done = done.fetch cat do
      log.error "unknown category"
      return
    end
    if !cat_done
      log.debug "no configured PVR"
      return
    end

    ok =
      case cat_done
      when :done then true
      else
        cat_done.fetch t.fetch("hash").downcase do
          log.warn "not found in PVR"
          return
        end
      end
    if !ok
      log.warn "not imported by PVR"
      return
    end

    st = SeedStats.new t

    log = log[st.seeding_done.yield_self { |done|
      {progress: Fmt.progress(st.progress)}.tap { |h|
        if !done || done == :ratio
          h[:ratio] = "%s of %s" \
            % [Fmt.ratio(st.ratio), Fmt.ratio(st.min_ratio)]
        end
        if !done || done == :time and (ts = [st.seed_time, st.time_limit]).any?
          h[:seed_time] = "%s of %s" % ts.map { |t| t ? Fmt.duration(t) : "?" }
        end
      }
    }]

    if !st.seeding_done
      log.debug "still seeding"
      return
    end

    log.info "done, deleting"
    qbt.delete_perm t
  end

  private def radarr_done(uri)
    pvr = Utils::PVR::Radarr.new uri, timeout: CONN_TIMEOUT, log: @log["radarr"]
    history_done pvr.history do |ev|
      ev.fetch "movieId"
    end
  end

  private def sonarr_done(uri)
    pvr = Utils::PVR::Sonarr.new uri, timeout: CONN_TIMEOUT, log: @log["sonarr"]
    history_done pvr.history do |ev|
      %w( seriesId episodeId ).map { |k| ev.fetch k }
    end
  end

  private def history_done(hist)
    done = {}
    by_id = {}

    hist.
      sort_by { |ev| ev.fetch "date" }.
      each { |ev|
        hash = (cl = ev.fetch("data")["downloadClient"] \
          and cl.downcase == "qbittorrent" \
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

    done.transform_values do |id|
      by_id.fetch id
    end
  end

  ImportStatus = Struct.new :date, :ok

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
  end
end

class SeedStats
  DEFAULT_MIN_RATIO = 10

  def initialize(t)
    @progress = t.fetch "progress"
    @ratio = t.fetch "ratio"
    @min_ratio = t.fetch("max_ratio").
      yield_self { |r| r > 0 ? r : DEFAULT_MIN_RATIO }
    @time_limit = t.fetch("max_seeding_time").
      yield_self { |mins| mins * 60 if mins > 0 }
    @seed_time = (Time.now - Time.at(t.fetch "completion_on") if @progress >= 1)
    @seeding_done =
      case
      when @ratio >= @min_ratio
        :ratio
      when @time_limit && @seed_time && @seed_time >= @time_limit
        :time
      end
  end

  attr_reader \
    :progress,
    :ratio, :min_ratio,
    :time_limit, :seed_time,
    :seeding_done
end

if $0 == __FILE__
  require 'metacli'
  app = App.new Utils::Log.new($stderr, level: :info).tap { |log|
    log.level = :debug if ENV["DEBUG"] == "1"
  }
  MetaCLI.new(ARGV).run app
end
