require 'minitest/autorun'
require_relative '../main'

class ResumesTest < Minitest::Test
  def test_optimize
    r = Resumes.new
    r.pause.concat [
      Tor[id: 'pa', downloading?: true, state: 'stalledDL'],
      Tor[id: 'pb', downloading?: false, state: 'stalledDL'],
      Tor[id: 'pc', downloading?: false, state: 'pausedDL'],
      Tor[id: 'pd', downloading?: false, state: 'stalledDL'],
    ]
    r.resume.concat [
      Tor[id: 'ra', downloading?: false, state: 'DL'],
      Tor[id: 'rb', downloading?: true, state: 'DL'],
      Tor[id: 'rc', downloading?: true, state: 'stalledDL'],
      Tor[id: 'rd', downloading?: true, state: 'stalledDL'],
    ]
    r.optimize!

    assert_equal %w[pa rc], r.pause.map(&:id)
    assert_equal %w[ra pc], r.resume.map(&:id)
  end

  Tor = Struct.new(:id, :downloading?, :state, keyword_init: true)
end
