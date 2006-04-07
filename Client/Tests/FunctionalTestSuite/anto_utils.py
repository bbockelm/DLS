
class SimpleConfigParser:
  def __init__(self):
      self.vars = {}

  def read(self, filename):

      # May raise IOError
      fp = open(filename)
         
      while(True):
        line = fp.readline()
        if not line:
            break
        if line.strip() == '' or line[0] in '#':
            continue
        tokens = line.split("=")
        key = tokens[0].strip()
        key = key.strip("\"")
        value = tokens[1].strip()
        value = value.strip("\"")

        self.vars[key] = value
        
  def get(self, keyname):
      try:
         return self.vars[keyname]
      except KeyError:
         return None

  def list(self):
      return self.vars.keys()
