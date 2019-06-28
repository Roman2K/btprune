require 'timeout'
require 'utils'

class App
  def initialize(log)
    @log = log
  end

  def cmd_prune(qbt, radarr: nil, sonarr: nil)
    qbt = begin
      Timeout.timeout 2 do
        Utils::QBitTorrent.new URI(qbt), log: @log["qbt"]
      end
    rescue Timeout::Error
      @log.debug "qBitTorrent HTTP API seems unavailable, aborting"
      exit 0
    end

    done = {
      "radarr" => radarr,
      "sonarr" => sonarr,
      "lidarr" => nil,
      "" => nil,
    }.tap { |h|
      h.each do |cat, url|
        h[cat] = (send "#{cat}_done", URI(url) if url)
      end
    }

    qbt.completed.each do |t|
      auto_delete qbt, t, done
    end
  end

  DEFAULT_MIN_RATIO = 10

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

    ok = cat_done.fetch t.fetch("hash").downcase do
      log.warn "not found in PVR"
      return
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
    history_done Radarr.new(uri, @log["radarr"]).history do |ev|
      ev.fetch "movieId"
    end
  end

  private def sonarr_done(uri)
    history_done Sonarr.new(uri, @log["sonarr"]).history do |ev|
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
      ("%.1f" % r).sub /\.0$/, ""
    end

    def self.progress(f)
      ("%.1f%%" % [f*100]).sub /\.0%$/, "%"
    end
  end
end

class SeedStats
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

class PVR
  def initialize(uri, log)
    @uri = uri
    @log = log
  end

  def history
    fetch_all(add_uri("/history", page: 1)).to_a
  end

  protected def fetch_all(uri)
    return enum_for __method__, uri unless block_given?
    fetched = 0
    total = nil
    uri = Utils.merge_uri uri, pageSize: 200
    loop do
      Hash[URI.decode_www_form(uri.query || "")].
        slice("page", "pageSize").
        tap { |h| @log.debug "fetching %p of %s" % [h, uri] }
      resp = get_response! uri
      data = JSON.parse resp.body
      total = data.fetch "totalRecords"
      page = data.fetch "page"
      if fetched <= 0 && page > 1
        fetched = data.fetch("pageSize") * (page - 1)
      end
      records = data.fetch "records"
      fetched += records.size
      @log.debug "fetch result: %p" \
        % {total: total, page: page, fetched: fetched, records: records.size}
      break if records.empty?
      records.each { |r| yield r }
      break if fetched >= total
      uri = Utils.merge_uri uri, page: page + 1
    end
  end

  protected def add_uri(*args, &block)
    Utils.merge_uri @uri, *args, &block
  end

  protected def get_response!(uri)
    Net::HTTP.get_response(uri).tap do |resp|
      resp.kind_of? Net::HTTPSuccess \
        or raise "unexpected response: %p (%s)" % [resp, resp.body]
    end
  end
end

class Radarr < PVR
end

class Sonarr < PVR
end

if $0 == __FILE__
  require 'metacli'
  app = App.new Utils::Log.new($stderr, level: :info).tap { |log|
    log.level = :debug if ENV["DEBUG"] == "1"
  }
  MetaCLI.new(ARGV).run app
end
