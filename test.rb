require 'minitest/autorun'
require_relative 'main'

class SeedStatsTest < Minitest::Test
  def test_seeding_done
    stats = SeedStats.new \
      "progress" => 0.9,
      "ratio" => 11,
      "max_ratio" => 10,
      "max_seeding_time" => 60, # minutes
      "completion_on" => Time.now - 61*60
    done, score = stats.seeding_done
    refute done

    # ratio reached, seeding time reached
    stats = SeedStats.new \
      "progress" => 1,
      "ratio" => 11,
      "max_ratio" => 10,
      "max_seeding_time" => 60, # minutes
      "completion_on" => Time.now - 61*60
    done, score = stats.seeding_done
    assert done
    assert score > 200

    # ratio not reached, seeding time reached
    stats = SeedStats.new \
      "progress" => 1,
      "ratio" => 0.1,
      "max_ratio" => 10,
      "max_seeding_time" => 60, # minutes
      "completion_on" => Time.now - 61*60
    done, score = stats.seeding_done
    assert done
    assert score > 100
    assert score < 200

    # ratio > 50%, seeding time > 50%
    stats = SeedStats.new \
      "progress" => 1,
      "ratio" => 5.1,
      "max_ratio" => 10,
      "max_seeding_time" => 60, # minutes
      "completion_on" => Time.now - 30*60
    done, score = stats.seeding_done
    assert done
    assert score > 100
    assert score < 200
  end
end
