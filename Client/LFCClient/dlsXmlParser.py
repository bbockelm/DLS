#!/usr/bin/env python

from xml.sax import ContentHandler, make_parser
from dlsDataObjects import DlsLocation, DlsFileBlock, DlsEntry, DlsDataObjectError


class EntryPageHandler(ContentHandler):

  def __init__(self):
    self.inBlock = False
    self.mapping = []
    self.fbAttrs = {}
    self.seAttrs = {}
 
 
  def startElement(self, name, attributes):
  
    if name == "block":
      self.ses = []
      self.fbAttrs = {}
      for attr in attributes.keys():
        if (attr == "name"): self.fbName = attributes["name"]
        else:
           self.fbAttrs[attr] = attributes[attr]
         
    elif name == "replica":
      self.seAttrs = {}
      for attr in attributes.keys():
        if (attr == "storage_element"): self.seName = attributes["storage_element"]
        else:
           self.seAttrs[attr] = attributes[attr]
      self.ses.append(DlsLocation(self.seName, self.seAttrs))
 
 
  def endElement(self, name):
    if name == "block":
      self.mapping.append(DlsEntry(DlsFileBlock(self.fbName, self.fbAttrs), self.ses))



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
      self.host = attributes["se_name"]
 
  def endElement(self, name):
    if name == "node":
      if self.host:
         if not (self.host in self.list):
            self.list.append(self.host)




class DlsXmlParser:
  def __init__(self):
    self.result = []
    self.handler = EntryPageHandler()
    self.handler_blocks = BlockPageHandler()
    self.handler_locs = NodePageHandler()
    self.parser = make_parser()

  def xmlToEntries(self, xmlSource):
    """
    Returns a list of DlsEntry objects holding the FileBlock and location
    information contained in the specified XML source (in PhEDEx's blockReplicas
    format)

    @param xmlSource: XML source file in URL format (e.g. http://...)

    @return: a list of DlsEntry objects with FileBlock and locations information
    """
    self.parser.setContentHandler(self.handler)
    self.parser.parse(xmlSource)
    return self.handler.mapping
    

  def xmlToBlocks(self, xmlSource):
    """
    Returns a list of DlsFileBlock objects holding the FileBlock information
    contained in the specified XML source (in PhEDEx's blockReplicas format)

    @param xmlSource: XML source file in URL format (e.g. http://...)

    @return: a list of DlsFileBlock objects with FileBlock location information
    """
    self.parser.setContentHandler(self.handler_blocks)
    self.parser.parse(xmlSource)
    return self.handler_blocks.mapping


  def xmlToLocations(self, xmlSource):
    """
    Returns a list of DlsLocation objects holding the location information
    contained in the specified XML source (in PhEDEx's "nodes" format)

    @param xmlSource: XML source file in URL format (e.g. http://...)

    @return: a list of DlsLocation objects with location information
    """
    self.parser.setContentHandler(self.handler_locs)
    self.parser.parse(xmlSource)
    hostList = self.handler_locs.list
    hostList.sort()
    return map(DlsLocation, hostList)


