require 'minitest/autorun'
require_relative '../main'

class SeedStatsTest < Minitest::Test
  def test_compute_seeding_score
    with_const SeedStats,
      MIN_SEED_RATIO: 10,
      MIN_SEED_MAX_SIZE: 15 * 1024**3,
      SEED_TIME_LIMIT: 4 * 86400 \
    do
      score = -> ratio, seed_time, size do
        st = SeedStats.allocate
        st.instance_variable_set :@ratio, ratio
        st.instance_variable_set :@seed_time, seed_time
        val, = st.__send__ :compute_seeding_score, size
        val
      end
      assert_equal 0.9, score.(9, 0, 1024)
      assert_equal 9.0, score.(9, 0, 150 * 1024**3)
      assert_equal 2.0, score.(10, 0, 30 * 1024**3)

      assert_equal 0.1, score.(1, 0, 1024)
      assert_equal 0.5, score.(1, 2 * 86400, 1024)
      assert_equal 1.0, score.(1, 4 * 86400, 1024)
    end
  end

  private def with_const(mod, values)
    silence_warnings = -> &block do
      orig, $VERBOSE = $VERBOSE, nil
      begin
        block.()
      ensure
        $VERBOSE = orig
      end
    end
    old = {}
    values.each do |name, val|
      old[name] = mod.const_get name
      silence_warnings.() { mod.const_set name, val }
    end
    begin
      yield
    ensure
      old.each do |name, val|
        silence_warnings.() { mod.const_set name, val }
      end
    end
  end
end
