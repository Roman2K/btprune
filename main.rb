require 'qbittorrent'
require 'log'

$log = Log.new $stderr, level: :info

module Commands
  extend self

  MIN_RATIO = 10

  def cmd_prune(qbt, radarr: nil, sonarr: nil)
    qbt = begin
      Timeout.timeout 2 do
        QBitTorrent.new URI(qbt), log: $log
      end
    rescue Timeout::Error
      $log.warn "qBitTorrent HTTP API seems unavailable, aborting"
      exit 0
    end

    torrents = qbt.completed
    done = {"radarr" => radarr, "sonarr" => sonarr}.tap do |h|
      h.each do |cat, url|
        h[cat] = (send "#{cat}_done", URI(url) if url)
      end
    end

    torrents.each do |t|
      auto_delete qbt, t, done
    end
  end

  private def auto_delete(qbt, t, done)
    name = t.fetch "name"
    cat = t.fetch "category"
    log = $log["in %s" % cat, torrent: name]
    cat_done = done.fetch cat do
      log.error "unknown category"
      return
    end
    cat_done or return

    ok = cat_done.fetch(t.fetch "hash") do
      log.warn "not found in PVR"
      return
    end
    if !ok
      log.warn "not imported by PVR"
      return
    end

    progress = t.fetch "progress"
    ratio = t.fetch "ratio"
    log = log[
      progress: ("%.1f%%" % [progress*100]).sub(/\.0%$/, "%"),
      ratio: ("%.1f" % ratio).sub(/\.0$/, ""),
    ]
    if progress < 1 || ratio < MIN_RATIO
      log.debug "still seeding"
      return
    end

    log.info "done, deleting"
    qbt.delete_perm t
  end

  private def radarr_done(uri)
    history_done Radarr.new(uri).history do |ev|
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
          and ev["downloadId"]) or next
        done[hash.downcase] = yield ev
      }
    done
  end

  private def sonarr_done(uri)
    sonarr = Sonarr.new uri

    done = history_done sonarr.history do |ev|
      ev.fetch("episode").fetch "hasFile"
    end

    mismatch = {}
    sonarr.queue.each do |entry|
      entry.fetch("protocol") == "torrent" or next
      name = entry.fetch "title"
      hash = entry.fetch("downloadId").downcase
      ok = entry.fetch("episode").fetch("hasFile")
      val = done[hash]
      if !val.nil? && ok != val
        mismatch[hash] ||= begin
          $log.warn "hasFile mismatch: importing? (%s)" % [name]
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
  def initialize(uri)
    @uri = uri
  end

  protected def fetch_all(uri)
    return enum_for __method__, uri unless block_given?
    fetched = 0
    total = nil
    uri = QBitTorrent.merge_uri uri, pageSize: 200
    loop do
      Hash[URI.decode_www_form(uri.query || "")].
        slice("page", "pageSize").
        tap { |h| $log.debug "fetching %p of %s" % [h, uri] }
      resp = Net::HTTP.get_response uri
      resp.kind_of? Net::HTTPSuccess \
        or raise "unexpected response: %p (%s)" % [resp, resp.body]
      data = JSON.parse resp.body
      total = data.fetch "totalRecords"
      page = data.fetch "page"
      if fetched <= 0 && page > 1
        fetched = data.fetch("pageSize") * (page - 1)
      end
      records = data.fetch "records"
      fetched += records.size
      $log.debug "fetch result: %p" \
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
end

class Radarr < PVR
  def history
    fetch_all(add_uri("/history", page: 1)).to_a
  end
end

class Sonarr < PVR
  def history
    fetch_all(add_uri("/history", page: 1)).to_a
  end

  def queue
    uri = add_uri "/queue"
    resp = Net::HTTP.get_response uri
    resp.kind_of? Net::HTTPSuccess \
      or raise "unexpected response: %p (%s)" % [resp, resp.body]
    JSON.parse resp.body
  end
end

if $0 == __FILE__
  require 'metacli'
  MetaCLI.new(ARGV).run Commands
end
