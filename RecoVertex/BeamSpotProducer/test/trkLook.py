from ROOT               import TFile, TTree, gDirectory, TH1F, TH2F, TCanvas, TLegend
from DataFormats.FWLite import Events

# define track selection
trk_MinpT_         = 1.0
trk_MaxEta_        = 2.4
trk_MaxIP_         = 1.0
trk_MaxZ_          = 60
trk_MinNTotLayers_ = 6
trk_MinNPixLayers_ = -1
trk_MaxNormChi2_   = 20
min_Ntrks_         = 50
convergence_       = 0.9
inputBeamWidth_    = -1


def selectTrk(ev):
  if  ev.nTotLayerMeas   >= trk_MinNTotLayers_   \
  and ev.nPixelLayerMeas >= trk_MinNPixLayers_   \
  and ev.normchi2        <  trk_MaxNormChi2_     \
  and abs( ev.d0 )       <  trk_MaxIP_           \
  and abs( ev.z0 )       <  trk_MaxZ_            \
  and ev.pt              >  trk_MinpT_           \
  and abs( ev.eta )      <  trk_MaxEta_:
    return 1

  else:
    return 0

class track(object):
  def __init__(self, event):
     self.nTotLayerMeas   = event.nTotLayerMeas   
     self.nPixelLayerMeas = event.nPixelLayerMeas 
     self.normchi2        = event.normchi2        
     self.pt              = event.pt              
     self.d0              = event.d0              
     self.z0              = event.z0              
     self.eta             = event.eta             
     self.phi0            = event.phi0
     self.d0              = event.d0
     self.good            = selectTrk(event)


def fillHistos(tree    ,
               hlumi   , 
               hntracks,
               hgtracks,
               hpixL   ,
               htotL   ,
               hchi2   ,
               hpt     ,  
               hd0     ,   
               hz0     ,  
               heta    ,
               hphid0  ,       
               hpixL_g ,
               htotL_g ,
               hchi2_g ,
               hpt_g   ,  
               hd0_g   ,   
               hz0_g   ,  
               heta_g  ,
               hphid0_g       
               ):

  lumi_ntrks = {}

  for i, ev in enumerate(tree):
#     if i > 1000: break
    try:
      lumi_ntrks[ev.lumi]
    except:
      lumi_ntrks[ev.lumi] = []  
    
    mytrack = track(ev)
          
    lumi_ntrks[ev.lumi].append(mytrack)

    htotL .Fill (mytrack.nTotLayerMeas   )
    hpixL .Fill (mytrack.nPixelLayerMeas )
    hchi2 .Fill (mytrack.normchi2        )
    hpt   .Fill (mytrack.pt              )
    hd0   .Fill (mytrack.d0              )
    hz0   .Fill (mytrack.z0              )
    heta  .Fill (mytrack.eta             )
    hphid0.Fill (mytrack.phi0, mytrack.d0)

    if selectTrk(ev):
      htotL_g .Fill (mytrack.nTotLayerMeas   )
      hpixL_g .Fill (mytrack.nPixelLayerMeas )
      hchi2_g .Fill (mytrack.normchi2        )
      hpt_g   .Fill (mytrack.pt              )
      hd0_g   .Fill (mytrack.d0              )
      hz0_g   .Fill (mytrack.z0              )
      heta_g  .Fill (mytrack.eta             )
      hphid0_g.Fill (mytrack.phi0, mytrack.d0)
      
  for k, v in lumi_ntrks.items():
    hntracks.Fill(k, len(v))
    goods = [tr for tr in v if tr.good]
    hgtracks.Fill(k, len(goods))


  


file_pcl = TFile.Open('run207231_PCLReplay_addCombinedResultsTree.root', 'r')
tree_pcl = file_pcl.Get('mytree')

file_se = TFile.Open('run207231_StreamExpress_addCombinedResultsTree.root', 'r')
tree_se = file_se.Get('mytree')

list_pcl = []
list_se  = []

print 'files opened'

# pcl histos
hlumi_pcl       = TH1F ('hlumi_pcl'      , 'hlumi_pcl'      ,   21,   690,  711)
hntracks_pcl    = TH2F ('hntracks_pcl'   , 'hntracks_pcl'   ,   21,   690,  711, 2000, 10000, 50000)
hgtracks_pcl    = TH2F ('hgtracks_pcl'   , 'hgtracks_pcl'   ,   21,   690,  711, 2000, 10000, 50000)
hphi_d0_pcl     = TH2F ('hphi_d0_pcl'    , 'hphi_d0_pcl'    ,  314, -3.14, 3.14, 500, -0.5, 0.5)
hphi_d0_g_pcl   = TH2F ('hphi_d0_g_pcl'  , 'hphi_d0_g_pcl'  ,  314, -3.14, 3.14, 500, -0.5, 0.5)
   
hpixL_pcl       = TH1F ('hpixL_pcl'      , 'hpixL_pcl'      ,   20,   0,  20)
htotL_pcl       = TH1F ('htotL_pcl'      , 'htotL_pcl'      ,   20,   0,  20)
hchi2_pcl       = TH1F ('hchi2_pcl'      , 'hchi2_pcl'      ,  100,   0,  10)
hpt_pcl         = TH1F ('hpt_pcl'        , 'hpt_pcl'        ,   70,   0, 100)
hd0_pcl         = TH1F ('hd0_pcl'        , 'hd0_pcl'        ,  100,   0,  10)
hz0_pcl         = TH1F ('hz0_pcl'        , 'hz0_pcl'        ,  200, -20,  20)
heta_pcl        = TH1F ('heta_pcl'       , 'heta_pcl'       ,   60,  -3,   3)

hpixL_good_pcl  = TH1F ('hpixL_good_pcl' , 'hpixL_good_pcl' ,   20,   0,  20)
htotL_good_pcl  = TH1F ('htotL_good_pcl' , 'htotL_good_pcl' ,   20,   0,  20)
hchi2_good_pcl  = TH1F ('hchi2_good_pcl' , 'hchi2_good_pcl' ,  100,   0,  10)
hpt_good_pcl    = TH1F ('hpt_good_pcl'   , 'hpt_good_pcl'   ,   70,   0, 100)
hd0_good_pcl    = TH1F ('hd0_good_pcl'   , 'hd0_good_pcl'   ,  100,   0,  10)
hz0_good_pcl    = TH1F ('hz0_good_pcl'   , 'hz0_good_pcl'   ,  200, -20,  20)
heta_good_pcl   = TH1F ('heta_good_pcl'  , 'heta_good_pcl'  ,   60,  -3,   3)

# stream express histos
hlumi_se        = TH1F ('hlumi_se'       , 'hlumi_se'       ,   21, 690,  711)
hntracks_se     = TH2F ('hntracks_se'    , 'hntracks_se'    ,   21, 690,  711, 2000, 10000, 50000)
hgtracks_se     = TH2F ('hgtracks_se'    , 'hgtracks_se'    ,   21, 690,  711, 2000, 10000, 50000)
hphi_d0_se      = TH2F ('hphi_d0_se'     , 'hphi_d0_se'     ,  314, -3.14, 3.14, 500, -0.5, 0.5)
hphi_d0_g_se    = TH2F ('hphi_d0_g_se'   , 'hphi_d0_g_se'   ,  314, -3.14, 3.14, 500, -0.5, 0.5)
         
hpixL_se        = TH1F ('hpixL_se'       , 'hpixL_se'       ,   20,   0,  20)
htotL_se        = TH1F ('htotL_se'       , 'htotL_se'       ,   20,   0,  20)
hchi2_se        = TH1F ('hchi2_se'       , 'hchi2_se'       ,  100,   0,  10)
hpt_se          = TH1F ('hpt_se'         , 'hpt_se'         ,   70,   0, 100)
hd0_se          = TH1F ('hd0_se'         , 'hd0_se'         ,  100,   0,  10)
hz0_se          = TH1F ('hz0_se'         , 'hz0_se'         ,  200, -20,  20)
heta_se         = TH1F ('heta_se'        , 'heta_se'        ,   60,  -3,   3)

hpixL_good_se   = TH1F ('hpixL_good_se'  , 'hpixL_good_se'  ,   20,   0,  20)
htotL_good_se   = TH1F ('htotL_good_se'  , 'htotL_good_se'  ,   20,   0,  20)
hchi2_good_se   = TH1F ('hchi2_good_se'  , 'hchi2_good_se'  ,  100,   0,  10)
hpt_good_se     = TH1F ('hpt_good_se'    , 'hpt_good_se'    ,   70,   0, 100)
hd0_good_se     = TH1F ('hd0_good_se'    , 'hd0_good_se'    ,  100,   0,  10)
hz0_good_se     = TH1F ('hz0_good_se'    , 'hz0_good_se'    ,  200, -20,  20)
heta_good_se    = TH1F ('heta_good_se'   , 'heta_good_se'   ,   60,  -3,   3)


hpixL_se.GetXaxis().SetTitle('# pixel layer')
htotL_se.GetXaxis().SetTitle('# total layer')
hchi2_se.GetXaxis().SetTitle('# norm chi2'  )
hpt_se  .GetXaxis().SetTitle('p_{T}'        )
hd0_se  .GetXaxis().SetTitle('d0'           )
hz0_se  .GetXaxis().SetTitle('z0'           )
heta_se .GetXaxis().SetTitle('#eta'         )

print 'histos created'

fillHistos(tree_pcl       , 
           hlumi_pcl      , 
           hntracks_pcl   ,
           hgtracks_pcl   ,
           hpixL_pcl      ,
           htotL_pcl      ,
           hchi2_pcl      ,
           hpt_pcl        ,
           hd0_pcl        ,
           hz0_pcl        ,
           heta_pcl       ,
           hphi_d0_pcl    ,
           hpixL_good_pcl ,
           htotL_good_pcl ,
           hchi2_good_pcl ,
           hpt_good_pcl   ,
           hd0_good_pcl   ,
           hz0_good_pcl   ,
           heta_good_pcl  ,
           hphi_d0_g_pcl
           )

print 'filled 1'

fillHistos(tree_se        , 
           hlumi_se       , 
           hntracks_se    ,
           hgtracks_se    ,
           hpixL_se       ,
           htotL_se       ,
           hchi2_se       ,
           hpt_se         ,
           hd0_se         ,
           hz0_se         ,
           heta_se        ,
           hphi_d0_se     ,
           hpixL_good_se  ,
           htotL_good_se  ,
           hchi2_good_se  ,
           hpt_good_se    ,
           hd0_good_se    ,
           hz0_good_se    ,
           heta_good_se   ,
           hphi_d0_g_se   ,
           )

print 'filled 2'

list_pcl.extend((hpixL_pcl,           htotL_pcl,      hchi2_pcl,      hpt_pcl,      hd0_pcl,      hz0_pcl,      heta_pcl))
list_pcl.extend((hpixL_good_pcl, htotL_good_pcl, hchi2_good_pcl, hpt_good_pcl, hd0_good_pcl, hz0_good_pcl, heta_good_pcl))

list_se.extend ((    hpixL_se,       htotL_se,       hchi2_se,       hpt_se,       hd0_se,       hz0_se,       heta_se ))
list_se.extend((hpixL_good_se,  htotL_good_se,  hchi2_good_se,  hpt_good_se,  hd0_good_se,  hz0_good_se,  heta_good_se ))

for i in list_pcl:
  i.SetLineColor(2)
  


outfile = TFile.Open('histos_ReplayVsStreamExpress.root', 'recreate')
outfile.cd()
for i in list_pcl:
  i.Write()
for i in list_se:
  i.Write()
hntracks_pcl  .Write()
hgtracks_pcl  .Write()
hphi_d0_pcl   .Write()
hphi_d0_g_pcl .Write()
hntracks_se   .Write()
hgtracks_se   .Write()
hphi_d0_se    .Write()
hphi_d0_g_se  .Write()

outfile.Close() 
   
# file.Close()