$:.unshift __dir__ + '/..'
require 'minitest/autorun'
require 'main'

class SeedStatsTest < Minitest::Test
  def test_compute_seeding_score
    t = Struct.new(:size).new(10 * 1024**3)
    assert_equal 10, SeedStats.compute_seeding_score(t)

    t = Struct.new(:size).new(16 * 1024**3)
    assert_equal 9.375, SeedStats.compute_seeding_score(t)
  end
end
