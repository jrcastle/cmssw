import ROOT
from ROOT import TFile, TTree, gDirectory, TH1F, TH2F, TCanvas, TLegend, gPad, gStyle, TGaxis

gStyle.SetTitleAlign(23)
TGaxis.SetMaxDigits(4)

c = TCanvas('', '', 600,900)
c.cd()

plotAllEvents = True
track_histos  = False
pv_histos     = True

if   track_histos: file = TFile.Open('histos_ReplayVsStreamExpress.root'  , 'r')
elif pv_histos:    file = TFile.Open('histosPV_ReplayVsStreamExpress.root', 'r')

if plotAllEvents:

  gStyle.SetOptStat('emr')
  stackPad = ROOT.TPad('stackPad', 'stackPad', 0.,  .3, 1., 1.  , 0, 0)  
  ratioPad = ROOT.TPad('ratioPad', 'ratioPad', 0., 0. , 1.,  .38, 0, 0)  

  c.cd()
  stackPad.Draw()
  ratioPad.Draw()
            
  if pv_histos:
    TGaxis.SetMaxDigits(3)
    variables = [
      ('hpvx'    , 'PV x [cm]' , '# selected PV' , (  0.0,  0.13), (0.69, 0.89, 0.55, 0.65), (-0.5, 0.5), 4, False),
      ('hpvy'    , 'PV y [cm]' , '# selected PV' , (  0.0,  0.13), (0.69, 0.89, 0.55, 0.65), (-0.5, 0.5), 4, False),
      ('hpvz'    , 'PV z [cm]' , '# selected PV' , ( -20  ,  20 ), (0.69, 0.89, 0.55, 0.65), (-0.5, 0.5), 4, False),
    ]

  elif track_histos:
    variables = [
      ('hpixL'    , '# pixel layer' , '# tracks' , (  0,   7), (0.69, 0.89, 0.55, 0.65), (-0.1 , 0.5  ), 1, False),
      ('htotL'    , '# total layer' , '# tracks' , (  3,  20), (0.69, 0.89, 0.55, 0.65), (-0.1 , 1    ), 1, False),
      ('hchi2'    , '# norm chi2'   , '# tracks' , (  0,   6), (0.69, 0.89, 0.55, 0.65), (-1   , 0.5  ), 1, False),
      ('hpt'      , 'p_{T} [GeV]'   , '# tracks' , (  0, 100), (0.69, 0.89, 0.55, 0.65), (-0.5 , 1    ), 1,  True),
      ('hd0'      , 'd0 [cm]'       , '# tracks' , (  0,  10), (0.69, 0.89, 0.55, 0.65), (0    , 0.5  ), 1,  True),
      ('hz0'      , 'z0 [cm]'       , '# tracks' , (-20,  20), (0.69, 0.89, 0.55, 0.65), (0.   , 0.65 ), 1, False),
      ('heta'     , '#eta'          , '# tracks' , ( -3,   3), (0.69, 0.89, 0.55, 0.65), (0.   , 0.2  ), 1, False)
#       ('hpixL'    , '# pixel layer' , '# tracks' , (  0,   7), (0.69, 0.89, 0.55, 0.65), (-0.1 , 0.5 ), 1, False),
#       ('htotL'    , '# total layer' , '# tracks' , (  3,  20), (0.69, 0.89, 0.55, 0.65), (-0.1 , 0.2 ), 1, False),
#       ('hchi2'    , '# norm chi2'   , '# tracks' , (  0,   6), (0.69, 0.89, 0.55, 0.65), (-2   , 0.2 ), 1, False),
#       ('hpt'      , 'p_{T} [GeV]'   , '# tracks' , (  0, 100), (0.69, 0.89, 0.55, 0.65), (-0.1 , 0.5 ), 1,  True),
#       ('hd0'      , 'd0 [cm]'       , '# tracks' , (  0,   2), (0.69, 0.89, 0.55, 0.65), ( 0   , 0.4 ), 1,  True),
#       ('hz0'      , 'z0 [cm]'       , '# tracks' , (-20,  20), (0.69, 0.89, 0.55, 0.65), (-0.1 , 0.5 ), 1, False),
#       ('heta'     , '#eta'          , '# tracks' , ( -3,   3), (0.69, 0.89, 0.55, 0.65), ( 0.  ,  .1 ), 1, False)
    ]

  else:
    print 'no category selected'
    exit()    

  
  for var in variables:

    h_se  = file.Get(var[0] + '_se' )
    h_pcl = file.Get(var[0] + '_pcl')
    
    h_se .Rebin(var[6])
    h_pcl.Rebin(var[6])
  
    h_se.SetTitle('Run 207231, LS 690-710')
    h_se.SetLineColor(ROOT.kBlack)
    h_se.GetYaxis().SetTitleOffset(1.35)
  
    h_se.GetXaxis().SetTitle(var[1])
    h_se.GetYaxis().SetTitle(var[2])
    h_se.GetXaxis().SetRangeUser(var[3][0], var[3][1])
    
    stackPad.cd()
    stackPad.SetBottomMargin(0.2)
    stackPad.SetLogy(var[7])
    h_se.Draw()
    c.Update()
    c.Modified()
    
    stats = gPad.GetPrimitive('stats')
    stats.SetBorderSize(0)
    stats.SetY1NDC(0.65)
    stats.SetY2NDC(0.77)
    stats.SetX1NDC(0.69)
    stats.SetX2NDC(0.89)
    stats.SetName('stat1')

    h_pcl.Draw('sames')
    c.Update()
    c.Modified()
    stats = gPad.GetPrimitive('stats')
    print 'stats', stats
    stats.SetBorderSize(0)
    stats.SetY1NDC(0.77)
    stats.SetY2NDC(0.89)
    stats.SetX1NDC(0.69)
    stats.SetX2NDC(0.89)
    stats.SetTextColor(2)

    l = TLegend(var[4][0], var[4][2], var[4][1], var[4][3])
    l.AddEntry(h_se,  'StreamExpress', 'l')
    l.AddEntry(h_pcl, 'Replay'       , 'l')
    l.SetBorderSize(0)
    l.Draw()
    
    ratioPad.cd()
    ratioPad.SetGridy(True)
    ratioPad.SetBottomMargin(0.2)
    h_se_clone  = h_se.Clone()
    h_pcl_clone = h_pcl.Clone()
    h_se_clone.Sumw2()
    h_pcl_clone.Sumw2()
    h_ratio = h_se_clone.Clone()
    for i in range(h_ratio.GetNbinsX()):
      n_pcl = h_pcl_clone.GetBinContent(i+1)
      n_se  = h_se_clone .GetBinContent(i+1)
      e_pcl = h_pcl_clone.GetBinError(i+1)
      e_se  = h_se_clone .GetBinError(i+1)
      h_ratio.SetBinContent(i+1, 1 - n_pcl/max(0.000001,n_se))
      h_ratio.SetBinError  (i+1, (min (e_pcl, e_se))/max(0.000001,n_se))
    h_ratio.GetYaxis().SetRangeUser( var[5][0], var[5][1])
    h_ratio.Draw('e')
    
    c.Update()
    c.Modified()
    stats = gPad.GetPrimitive('stat1')
    stats.SetBorderSize(0)
    stats.SetTextColor(0)
    stats.SetY1NDC(0.77)
    stats.SetY2NDC(0.77)
    stats.SetX1NDC(0.69)
    stats.SetX2NDC(0.69)
    h_ratio.SetTitle('')
    h_ratio.SetMarkerStyle(8)
    h_ratio.GetYaxis().SetNdivisions(6)
    h_ratio.GetYaxis().SetLabelSize(0.06)
    h_ratio.GetXaxis().SetLabelSize(0.06)
    h_ratio.GetYaxis().SetTitleSize(0.07)
    h_ratio.GetXaxis().SetTitleSize(0.07)
    h_ratio.GetYaxis().SetTitleOffset(0.7)
    h_ratio.GetYaxis().SetTitle('(SE - Replay)/SE')

    c.SetLogy(var[6])

    c.SaveAs('compare_' + var[0] + '_diffPad.pdf')




#  plot per LUMI
else:

  gStyle.SetOptStat('')
  TGaxis.SetMaxDigits(3)
 
  h_se  = file.Get('hnpv_se' )
  h_pcl = file.Get('hnpv_pcl')
#   h_se  = file.Get('hgtracks_se' )
#   h_pcl = file.Get('hgtracks_pcl')

  h_se .SetLineColor(ROOT.kBlack)
  h_pcl.SetLineColor(ROOT.kRed)

  h_se.GetXaxis().SetTitle('LS')
  h_se.GetYaxis().SetTitle('# selected PV')
  h_se.GetYaxis().SetTitleOffset(1.3)

  h_se.GetYaxis().SetRangeUser(2000, 6000)
  # h_se.GetYaxis().SetRangeUser(12000, 27000)
  # h_se.GetYaxis().SetRangeUser(25000, 50000)

  h_se .SetTitle('Run 207231')
  h_se .SetMarkerStyle(8)
  h_pcl.SetMarkerStyle(8)
  h_pcl.SetMarkerColor(ROOT.kRed)
  h_se .Draw('box')
  h_pcl.Draw('box sames')

  l = TLegend( 0.6, 0.73, 0.89, 0.85)
  # l = TLegend( 0.69, 0.65, 0.89, 0.75)
  l.AddEntry(h_se,  'StreamExpress', 'pel')
  l.AddEntry(h_pcl, 'Replay'       , 'pel')
  l.SetBorderSize(0)
  l.Draw()

  gPad.SetGridx()
  c.SaveAs('compare_gpv.pdf')
