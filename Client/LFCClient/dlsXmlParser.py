#!/usr/bin/env python

from xml.sax import ContentHandler, make_parser
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry, DlsDataObjectError, DlsFile



############################################
# Helper classes
# SAX handlers
############################################

class EntryPageHandler(ContentHandler):

  def __init__(self):
    self.mapping = []
    self.fbAttrs = {}
    self.seAttrs = {}
 
 
  def startElement(self, name, attributes):
    if name == "block":
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
 
 
  def endElement(self, name):
    if name == "block":
      self.mapping.append(DlsEntry(DlsFileBlock(self.fbName, self.fbAttrs), self.locs))



class BlockPageHandler(ContentHandler):
  def __init__(self):
    self.mapping = []
    self.fbAttrs = {}
 
  def startElement(self, name, attributes):
    if name == "block":
      for attr in attributes.keys():
        if (attr == "name"): self.fbName = attributes["name"]
        else:
           self.fbAttrs[attr] = attributes[attr]
 
  def endElement(self, name):
    if name == "block":
      self.mapping.append(DlsFileBlock(self.fbName, self.fbAttrs))



class NodePageHandler(ContentHandler):
  def __init__(self):
    self.host = ""
    self.list = []
 
  def startElement(self, name, attributes):
    if name == "node":
      self.host = attributes["se"]
 
  def endElement(self, name):
    if name == "node":
      if self.host and (self.host not in self.list):
            self.list.append(self.host)


class FilePageHandler(ContentHandler):

  def __init__(self):
    self.fbAttrs = {}
    self.result = []
    self.mapping = []
 
  def startElement(self, name, attributes):
    if name == "block":
      self.fbAttrs = {}
      self.files = {}
      for attr in attributes.keys():
        if (attr == "name"): self.fbName = attributes["name"]
        else:                self.fbAttrs[attr] = attributes[attr]

    if name == "file":
      self.ses = []
      self.locs = []
      self.fileName = attributes["name"]
         
    elif name == "replica":
      self.seName = attributes["se"]
      if self.seName and (self.seName not in self.ses):
         self.ses.append(self.seName)
         self.locs.append(DlsLocation(self.seName))
#      self.ses.append(self.seName)
 

  def endElement(self, name):
    if name == "file":
       self.files[ DlsFile(self.fileName) ] = self.locs
#       self.files[ self.fileName ] = self.ses
       
    if name == "block":
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
# Public interface
# class DlsXmlParser 
############################################

class DlsXmlParser:
  def __init__(self):
    self.result = []

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
    hostList = handler.list
    hostList.sort()
    return map(DlsLocation, hostList)


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
    return handler.mapping
