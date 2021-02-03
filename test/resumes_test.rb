require 'minitest/autorun'
require_relative '../main'

class ResumesTest < Minitest::Test
  ST_STALLED = Resumes::STATE_STALLED
  ST_PAUSED = 'pausedDL'
  ST_DL = 'DL'

  def test_optimize
    r = Resumes.new 1
    r.pause.concat [
      Tor[id: 'pa', downloading?: true, state: ST_STALLED],
      Tor[id: 'pb', downloading?: false, state: ST_STALLED],
      Tor[id: 'pc', downloading?: false, state: ST_PAUSED],
      Tor[id: 'pd', downloading?: false, state: ST_STALLED],
    ]
    r.resume.concat [
      Tor[id: 'ra', downloading?: false, state: ST_DL],
      Tor[id: 'rb', downloading?: true, state: ST_DL],
      Tor[id: 'rc', downloading?: true, state: ST_STALLED],
      Tor[id: 'rd', downloading?: true, state: ST_STALLED],
    ]
    r.optimize!

    assert_equal %w[pa rc], r.pause.map(&:id)
    assert_equal %w[ra pc], r.resume.map(&:id)
  end

  Tor = Struct.new(:id, :downloading?, :state, keyword_init: true)
end
