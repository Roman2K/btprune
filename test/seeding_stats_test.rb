$:.unshift __dir__ + '/..'
require 'minitest/autorun'
require 'main'

class SeedStatsTest < Minitest::Test
  def test_seeding
    max_ratio = 10
    max_seeding_time = 60 # minutes
    get_seeding = -> progress:, ratio:, compl_ago: do
      SeedStats.new(
        "state" => "uploading",
        "added_on" => Time.now - 2*60*60,
        "availability" => 1,
        "progress" => progress,
        "ratio" => ratio,
        "max_ratio" => max_ratio,
        "max_seeding_time" => max_seeding_time,
        "completion_on" => Time.now - compl_ago*60,
      ).seeding
    end

    # ratio reached, seeding time reached -- progress != 100%
    score = get_seeding[progress: 0.9, ratio: 11, compl_ago: 61]
    refute score.ok

    # ratio reached, seeding time reached
    score = get_seeding[progress: 1, ratio: 11, compl_ago: 61]
    assert score.ok
    assert score.to_f > 2.0

    # ratio not reached, seeding time reached
    score = get_seeding[progress: 1, ratio: 0.1, compl_ago: 61]
    assert score.ok
    assert score.to_f > 1.0
    assert score.to_f < 2.0

    # ratio > 50%, seeding time > 50%
    score = get_seeding[progress: 1, ratio: 5.1, compl_ago: 30]
    assert score.ok
    assert score.to_f > 1.0
    assert score.to_f < 2.0
  end

  def test_health
    opts = {
      dl_time_limit: 7*24*3600,
      dl_grace: 1*24*3600,
    }

    get_health = -> added_days_ago:, avail:, progress:,
      state: SeedStats::Statuses::STALLED_DL \
    do
      t = {
        "state" => state,
        "added_on" => Time.now - added_days_ago*24*3600,
        "availability" => avail,
        "progress" => progress,
        "ratio" => 0, "max_ratio" => 0, "max_seeding_time" => 0,
      }
      t["completion_on"] = Time.now if progress >= 1
      SeedStats.new(t, **opts).health
    end

    score = get_health[added_days_ago: 1, avail: 1, progress: 1.0]
    assert score.ok
    assert_equal 1, score.to_f

    score = get_health[added_days_ago: 1, avail: 1, progress: 1.0,
      state: SeedStats::Statuses::ERROR,
    ]
    assert score.ok
    assert_equal 1, score.to_f

    score = get_health[added_days_ago: 7, avail: 0.2, progress: 1.0]
    assert score.ok
    assert_equal 1, score.to_f

    score = get_health[added_days_ago: 0.9, avail: 0.1, progress: 0.0]
    assert score.ok
    assert score.to_f > 1.0
    assert score.to_f < 2.0

    score = get_health[added_days_ago: 0.9, avail: 0.1, progress: 0.0,
      state: SeedStats::Statuses::ERROR,
    ]
    refute score.ok

    score = get_health[added_days_ago: 1, avail: 0.1, progress: 0.0]
    assert !score.ok

    score = get_health[added_days_ago: 1, avail: 0.1, progress: 0.0,
      state: SeedStats::Statuses::DOWNLOADING,
    ]
    assert score.ok

    score = get_health[added_days_ago: 3, avail: 0.5, progress: 0.5]
    assert score.ok
    assert score.to_f > 1.0
    assert score.to_f < 2.0

    score = get_health[added_days_ago: 5, avail: 0.5, progress: 0.5]
    assert !score.ok

    score = get_health[added_days_ago: 6, avail: 0.8, progress: 0.5]
    assert score.ok
    assert score.to_f > 1.0
    assert score.to_f < 2.0

    score = get_health[added_days_ago: 7, avail: 0.8, progress: 0.5]
    assert !score.ok

    score = get_health[added_days_ago: 7, avail: 0.9, progress: 0.5]
    assert score.ok
    assert score.to_f > 1.0
    assert score.to_f < 2.0
  end
end
