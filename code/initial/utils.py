from collections import namedtuple
import unittest as ut
WINDOW = 5

class BallotNumber(namedtuple('BallotNumber',['round','leader_id'])):
  __slots__ = ()
  def __str__(self):
    return "BN(%d,%s)" % (self.round, str(self.leader_id))

  def __gt__(self, other):
    if other is None:
      return True
    else:
      return super().__gt__(other)


class PValue(namedtuple('PValue',['ballot_number',
                                  'slot_number',
                                  'command'])):
  __slots__ = ()
  def __str__(self):
    return "PV(%s,%s,%s)" % (str(self.ballot_number),
                             str(self.slot_number),
                             str(self.command))

class Command(namedtuple('Command',['client',
                                    'req_id',
                                    'op'])):
  __slots__ = ()
  def __str__(self):
    return "Command(%s,%s,%s)" % (str(self.client),
                                  str(self.req_id),
                                  str(self.op))

class ReconfigCommand(namedtuple('ReconfigCommand',['client',
                                                    'req_id',
                                                    'config'])):
  __slots__ = ()
  def __str__(self):
    return "ReconfigCommand(%s,%s,%s)" % (str(self.client),
                                          str(self.req_id),
                                          str(self.config))

class Config(namedtuple('Config',['replicas',
                                  'acceptors',
                                  'leaders'])):
  __slots__ = ()
  def __str__(self):
    return "%s;%s;%s" % (','.join(self.replicas),
                         ','.join(self.acceptors),
                         ','.join(self.leaders))


class test_ballot_number(ut.TestCase):
  def setUp(self):
    self.x = BallotNumber(2,1)
    self.y = BallotNumber(3,0)

  def test_compare_none(self):
    self.assertTrue(self.x > None)
  def test_compare_larger(self):
    self.assertTrue(self.y > self.x)
  def test_comapre_smaller(self):
    self.assertFalse(self.x > self.y)
    
if __name__ == "__main__":
  ut.main()
