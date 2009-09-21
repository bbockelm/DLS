#
# $Id$
#
# DLS XML parser (for PhEDEx back-end). $Name:  $.
# Antonio Delgado Peris. CIEMAT. CMS.
#

"""
This module implements the XML parser for the reponses from
a PhEDEx back-end for DLS API consumption. All methods and
classes here included are for internal DLS use only (no
public interface).
"""

############
# Imports
############
from xml.sax import ContentHandler, make_parser
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry, DlsDataObjectError, DlsFile
from dlsApiExceptions import DlsErrorWithServer



############################################
# Helper classes
# SAX handlers
############################################

class EntryPageHandler(ContentHandler):

  def __init__(self):
    self.phedexReply = False
    self.inError = False
    self.error = ""
    self.mapping = []
    self.fbAttrs = {}
    self.seAttrs = {}
 
 
  def startElement(self, name, attributes):
    if name == "error":
      self.inError = True

    elif name == "phedex":
      self.phedexReply = True
      
    elif name == "block":
      self.locs = []
      self.ses = []
      self.fbAttrs = {}
      for attr in attributes.keys():
        if (attr == "name"): self.fbName = attributes["name"]
        else:
           self.fbAttrs[attr] = attributes[attr]
         
    elif name == "replica":
      self.seAttrs = {}
      for attr in attributes.keys():
        if (attr == "se"): self.seName = attributes["se"]
        else:
           self.seAttrs[attr] = attributes[attr]
      if self.seName and (self.seName not in self.ses):
         self.ses.append(self.seName)
         self.locs.append(DlsLocation(self.seName, self.seAttrs))

  def characters(self, contents):
    if self.inError:
      self.error += contents
 
  def endElement(self, name):
    if name == "error":
      self.inError = False
      raise DlsErrorWithServer(self.error.strip())

    elif name == "block":
      self.mapping.append(DlsEntry(DlsFileBlock(self.fbName, self.fbAttrs), self.locs))



class BlockPageHandler(ContentHandler):
  def __init__(self):
    self.phedexReply = False
    self.inError = False
    self.error = ""
    self.mapping = []
    self.fbAttrs = {}
 
  def startElement(self, name, attributes):
    if name == "error":
      self.inError = True
      
    elif name == "phedex":
      self.phedexReply = True
      
    elif name == "block":
      for attr in attributes.keys():
        if (attr == "name"): self.fbName = attributes["name"]
        else:
           self.fbAttrs[attr] = attributes[attr]
 
  def characters(self, contents):
    if self.inError:
      self.error += contents
 
  def endElement(self, name):
    if name == "error":
      self.inError = False
      raise DlsErrorWithServer(self.error.strip())

    elif name == "block":
      self.mapping.append(DlsFileBlock(self.fbName, self.fbAttrs))



class NodePageHandler(ContentHandler):
  def __init__(self):
    self.phedexReply = False
    self.inError = False
    self.error = ""
    self.host = ""
    self.list = []
 
  def startElement(self, name, attributes):
    if name == "error":
      self.inError = True
      
    elif name == "phedex":
      self.phedexReply = True
      
    elif name == "node":
#      self.host = attributes["se"]
      self.seAttrs = {}
      for attr in attributes.keys():
        if (attr == "se"): self.host = attributes["se"]
        else:
           self.seAttrs[attr] = attributes[attr]
 
  def characters(self, contents):
    if self.inError:
      self.error += contents
 
  def endElement(self, name):
    if name == "error":
      self.inError = False
      raise DlsErrorWithServer(self.error.strip())

    elif name == "node":
#      if self.host and (self.host not in self.list):
#            self.list.append(self.host)
       self.list.append(DlsLocation(self.host, self.seAttrs))


class FilePageHandler(ContentHandler):

  def __init__(self):
    self.phedexReply = False
    self.inError = False
    self.error = ""
    self.fbAttrs = {}
    self.result = []
    self.mapping = []
 
  def startElement(self, name, attributes):
    if name == "error":
      self.inError = True
      
    elif name == "phedex":
      self.phedexReply = True
      
    elif name == "block":
      self.fbAttrs = {}
      self.files = {}
      for attr in attributes.keys():
        if (attr == "name"): self.fbName = attributes["name"]
        else:                self.fbAttrs[attr] = attributes[attr]

    elif name == "file":
      self.ses = []
      self.locs = []
      self.fileName = attributes["name"]
         
    elif name == "replica":
      self.seName = attributes["se"]
      if self.seName and (self.seName not in self.ses):
         self.ses.append(self.seName)
         self.locs.append(DlsLocation(self.seName))
#      self.ses.append(self.seName)
 

  def characters(self, contents):
    if self.inError:
      self.error += contents
 
  def endElement(self, name):
    if name == "error":
      self.inError = False
      raise DlsErrorWithServer(self.error.strip())

    elif name == "file":
       self.files[ DlsFile(self.fileName) ] = self.locs
#       self.files[ self.fileName ] = self.ses
       
    elif name == "block":
       self.mapping.append([DlsFileBlock(self.fbName, self.fbAttrs), self.files])


# This would be the OLD FilePageHandler (without duplicates filtering) with attribute support 
# But for now we're just getting name and host (below), as should be faster

#class FilePageHandler(ContentHandler):

#  def __init__(self):
#    self.mapping = {}
#    self.fileAttrs = {}
#    self.seAttrs = {}
 
 
#  def startElement(self, name, attributes):
#    if name == "file":
#      self.ses = []
#      self.fileAttrs = {}
#      for attr in attributes.keys():
#        if (attr == "name"): self.fileName = attributes["name"]
#        else:
#           self.fileAttrs[attr] = attributes[attr]
#         
#    elif name == "replica":
#      self.seAttrs = {}
#      for attr in attributes.keys():
#        if (attr == "se"): self.seName = attributes["se"]
#        else:
#           self.seAttrs[attr] = attributes[attr]
#      self.ses.append(DlsLocation(self.seName, self.seAttrs))
 
#  def endElement(self, name):
#    if name == "file":
#       self.mapping[ DlsFile(self.fileName, self.fileAttrs) ] = self.ses



############################################
# Class DlsXmlParser 
############################################

class DlsXmlParser:

  def xmlToEntries(self, xmlSource):
    """
    Returns a list of DlsEntry objects holding the FileBlock and location
    information contained in the specified XML source (in PhEDEx's blockReplicas
    format)

    @param xmlSource: XML source file in URL format (e.g. http://...) or file object

    @return: a list of DlsEntry objects with FileBlock and locations information
    """
    parser = make_parser()
    handler = EntryPageHandler()
    parser.setContentHandler(handler)
    parser.parse(xmlSource)
    if not handler.phedexReply:
      raise DlsErrorWithServer("No valid server response (no phedex entry). Check DLS endpoint")
    return handler.mapping
    

  def xmlToBlocks(self, xmlSource):
    """
    Returns a list of DlsFileBlock objects holding the FileBlock information
    contained in the specified XML source (in PhEDEx's blockReplicas format)

    @param xmlSource: XML source file in URL format (e.g. http://...) or file object

    @return: a list of DlsFileBlock objects with FileBlock information
    """
    parser = make_parser()
    handler = BlockPageHandler()
    parser.setContentHandler(handler)
    parser.parse(xmlSource)
    if not handler.phedexReply:
      raise DlsErrorWithServer("No valid server response (no phedex entry). Check DLS endpoint")
    return handler.mapping


  def xmlToLocations(self, xmlSource):
    """
    Returns a list of DlsLocation objects holding the location information
    contained in the specified XML source (in PhEDEx's "nodes" format)

    @param xmlSource: XML source file in URL format (e.g. http://...) or file object

    @return: a list of DlsLocation objects with location information
    """
    parser = make_parser()
    handler = NodePageHandler()
    parser.setContentHandler(handler)
    parser.parse(xmlSource)
    if not handler.phedexReply:
      raise DlsErrorWithServer("No valid server response (no phedex entry). Check DLS endpoint")
#    hostList = handler.list
#    hostList.sort()
#    return map(DlsLocation, hostList)
    return handler.list


  def xmlToFileLocs(self, xmlSource):
    """
    Returns a list of dict objects holding a DlsFile as key and a list of 
    DlsLocation objects as values for each DlsFile.
    contained in the specified XML source (in PhEDEx's "fileReplicas" format)

    @param xmlSource: XML source file in URL format (e.g. http://...) or file object

    @return: a list of dicts associating DlsFile objects and locations 
    """
    parser = make_parser()
    handler = FilePageHandler()
    parser.setContentHandler(handler)
    parser.parse(xmlSource)
    if not handler.phedexReply:
      raise DlsErrorWithServer("No valid server response (no phedex entry). Check DLS endpoint")
    return handler.mapping
