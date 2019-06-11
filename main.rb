require 'qbittorrent'
require 'log'

class App
  def initialize(log)
    @log = log
  end

  def cmd_prune(qbt, radarr: nil, sonarr: nil)
    qbt = begin
      Timeout.timeout 2 do
        QBitTorrent.new URI(qbt), log: @log["qbt"]
      end
    rescue Timeout::Error
      @log.warn "qBitTorrent HTTP API seems unavailable, aborting"
      exit 0
    end

    done = {"radarr" => radarr, "sonarr" => sonarr}.tap do |h|
      h.each do |cat, url|
        h[cat] = (send "#{cat}_done", URI(url) if url)
      end
    end

    qbt.completed.each do |t|
      auto_delete qbt, t, done
    end
  end

  DEFAULT_MIN_RATIO = 10

  private def auto_delete(qbt, t, done)
    cat = t.fetch "category"
    log = @log["in %s" % cat, torrent: t.fetch("name")]
    cat_done = done.fetch cat do
      log.error "unknown category"
      return
    end
    cat_done or return

    ok = cat_done.fetch t.fetch("hash").downcase do
      log.warn "not found in PVR"
      return
    end
    if !ok
      log.warn "not imported by PVR"
      return
    end

    progress = t.fetch "progress"
    if progress < 1
      log.debug "still downloading"
      return
    end

    ratio = t.fetch "ratio"
    min_ratio = t.fetch("max_ratio").
      yield_self { |r| r > 0 ? r : DEFAULT_MIN_RATIO }
    time_limit = t.fetch("max_seeding_time").
      yield_self { |mins| mins * 60 if mins > 0 }
    seed_time = (Time.now - Time.at(t.fetch "completion_on") if progress >= 1)
    seeding_done = ratio >= min_ratio \
      || (time_limit && seed_time && seed_time >= time_limit)

    log = log[{
      progress: Fmt.progress(progress),
      ratio: Fmt.ratio(ratio),
      min_ratio: Fmt.ratio(min_ratio),
      seed_time: (Fmt.duration seed_time if seed_time),
      time_limit: (Fmt.duration time_limit if time_limit),
    }.reject { |k,v| v.nil? }]

    if !seeding_done
      log.debug "still seeding"
      return
    end

    log.info "done, deleting"
    qbt.delete_perm t
  end

  private def radarr_done(uri)
    history_done Radarr.new(uri, @log["radarr"]).history do |ev|
      ev.fetch("movie").fetch "hasFile"
    end
  end

  private def history_done(hist)
    done = {}
    hist.
      sort_by { |ev| ev.fetch "date" }.
      each { |ev|
        hash = (cl = ev.fetch("data")["downloadClient"] \
          and cl.downcase == "qbittorrent" \
          and ev.fetch "downloadId") or next
        done[hash.downcase] = yield ev
      }
    done
  end

  private def sonarr_done(uri)
    sonarr = Sonarr.new uri, @log["sonarr"]

    done = history_done sonarr.history do |ev|
      ev.fetch("episode").fetch "hasFile"
    end

    mismatch = {}
    sonarr.queue.each do |entry|
      entry.fetch("protocol") == "torrent" or next
      log = @log[torrent: entry.fetch("title")]
      hash = entry.fetch("downloadId").downcase
      ok = entry.fetch("episode").fetch("hasFile")
      val = done[hash]
      if !val.nil? && ok != val
        mismatch[hash] ||= begin
          log.warn "hasFile mismatch: importing?"
          true
        end
        ok = false
      end
      done[hash] = ok
    end

    done
  end
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
    uri = QBitTorrent.merge_uri uri, pageSize: 200
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
      uri = QBitTorrent.merge_uri uri, page: page + 1
    end
  end

  protected def add_uri(*args, &block)
    QBitTorrent.merge_uri @uri, *args, &block
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
  def queue
    JSON.parse get_response!(add_uri "/queue").body
  end
end

module Fmt
  def self.duration(d)
    case
    when d < 60 then "%ds" % d
    when d < 3600 then m, d = d.divmod(60); "%dm%s" % [m, duration(d)]
    when d < 86400 then h, d = d.divmod(3600); "%dh%s" % [h, duration(d)]
    else ds, d = d.divmod(86400); "%dd%s" % [ds, duration(d)]
    end.sub /([a-z])(0[a-z])+$/, '\1'
  end
  
  def self.ratio(r)
    ("%.1f" % r).sub /\.0$/, ""
  end

  def self.progress(f)
    ("%.1f%%" % [f*100]).sub /\.0%$/, "%"
  end
end

if $0 == __FILE__
  require 'metacli'
  app = App.new Log.new($stderr, level: :info)
  MetaCLI.new(ARGV).run app
end
