def TextToString(text=''):
   '''
   Convert arbitrary text to a string variable.
   Accepts: NoneType, unicode, string, integer, and bytes
   Returns: string
   '''
   try:
      text = text.rstrip()
      text = text.decode('utf-8')
   except Exception as e:
      pass

   try:
      return str(text)
   except:
      return str(text).encode('utf-8')
