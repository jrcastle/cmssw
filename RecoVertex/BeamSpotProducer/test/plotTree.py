#!/usr/bin/env python
from ROOT import TTree, TFile, AddressOf, gROOT, TChain, TGraphErrors, TCanvas, TLegend
from array import array


def _getResults(thelist):
  thelumi      = array('d', thelist.keys())
  thelumierr   = array('d', [0.5  for i in thelist.values()])
  theval       = array('d', [i[0] for i in thelist.values()])
  thevalerr    = array('d', [i[1] for i in thelist.values()])
  
  return thelumi, thelumierr, theval, thevalerr




if __name__ == '__main__':

  f_old = TFile('run207231_StreamExpress_addCombinedResultsTree.root','READ')
  t_old = f_old.Get("combinedFitResults")

  f_new = TFile('run207231_PCLReplay_addCombinedResultsTree.root','READ')
  t_new = f_new.Get("combinedFitResults")

  myList_old = { }
  myList_new = { }


  print t_old.GetEntries()
  print t_new.GetEntries()

  for event in t_old:
    myList_old[event.beginLumi] = (event.z, event.zErr)

  for event in t_new:
    myList_new[event.beginLumi] = (event.z, event.zErr)
   
#   for i,j in myList_old.items():
#     print 'old: ' , i,j
#   for i,j in myList_new.items():
#     print 'new: ' , i,j


  thelumi_old, thelumierr_old, thex_old, thexerr_old = _getResults(myList_old)
  thelumi_new, thelumierr_new, thex_new, thexerr_new = _getResults(myList_new)

  g_old = TGraphErrors(len(myList_old), thelumi_old, thex_old, thelumierr_old, thexerr_old)
  g_old.SetMarkerStyle(22)
  g_old.SetMarkerSize(0.3)
  g_old.GetXaxis().SetTitle("z")

  g_new = TGraphErrors(len(myList_new), thelumi_new, thex_new, thelumierr_new, thexerr_new)
  g_new.SetMarkerStyle(22)
  g_new.SetMarkerSize(0.3)
  g_new.SetMarkerColor(2)
  g_new.SetLineColor(2)

  c = TCanvas ()
  g_old.Draw("AP")

#   c2 = TCanvas ()
  g_new.Draw("P" )
  
  l = TLegend(0.66,0.6,0.8,0.8)
  l.AddEntry(g_old, "StreamExpress", "pel")
  l.AddEntry(g_new, "Replay"       , "pel")
  l.Draw()
#   c.Update()







