from ROOT               import TFile, TTree, gDirectory, TH1F, TH2F, TCanvas, TLegend
from DataFormats.FWLite import Events

class pv(object):
  def __init__(self, event):
     self.x        = event.position[0]   
     self.y        = event.position[1] 
     self.z        = event.position[2]        
     self.xErr     = event.posError[0]              
     self.yErr     = event.posError[1]          
     self.zErr     = event.posError[2]          
     self.posCorr0 = event.posCorr[0]             
     self.posCorr1 = event.posCorr[1]
     self.posCorr2 = event.posCorr[2]


def fillHistos(tree    ,
               hlumi   , 
               hnpv,
               hpvx   ,
               hpvy   ,
               hpvz   
               ):

  lumi_npv = {}

  for i, ev in enumerate(tree):
    #if i > 100000: break
    try:
      lumi_npv[ev.lumi]
    except:
      lumi_npv[ev.lumi] = []  
    
    mypv = pv(ev)
          
    lumi_npv[ev.lumi].append(mypv)

    hpvy .Fill (mypv.x )
    hpvx .Fill (mypv.y )
    hpvz .Fill (mypv.z )
      
  for k, v in lumi_npv.items():
    hnpv.Fill(k, len(v))


file_pcl = TFile.Open('run207231_PCLReplay_addCombinedResultsTree.root', 'r')
tree_pcl = file_pcl.Get('PrimaryVertices')

file_se = TFile.Open('run207231_StreamExpress_addCombinedResultsTree.root', 'r')
tree_se = file_se.Get('PrimaryVertices')

list_pcl = []
list_se  = []

print 'files opened'

# pcl histos
hlumi_pcl   = TH1F ('hlumi_pcl' , 'hlumi_pcl'  ,   21,   690,  711)
hnpv_pcl    = TH2F ('hnpv_pcl'  , 'hnpv_pcl'   ,   21,  690,  711, 1000, 0, 10000)
   
hpvx_pcl    = TH1F ('hpvx_pcl'  , 'hpvx_pcl'   ,  1000, -0.2,  0.2)
hpvy_pcl    = TH1F ('hpvy_pcl'  , 'hpvy_pcl'   ,  1000, -0.2,  0.2)
hpvz_pcl    = TH1F ('hpvz_pcl'  , 'hpvz_pcl'   ,   400, -20,   20 )

# stream express histos
hlumi_se    = TH1F ('hlumi_se'  , 'hlumi_se'   ,   21,  690,  711)
hnpv_se     = TH2F ('hnpv_se'   , 'hnpv_se'    ,   21,  690,  711, 1000, 0, 10000)
         
hpvx_se     = TH1F ('hpvx_se'   , 'hpvx_se'    ,  1000, -0.2,  0.2)
hpvy_se     = TH1F ('hpvy_se'   , 'hpvy_se'    ,  1000, -0.2,  0.2)
hpvz_se     = TH1F ('hpvz_se'   , 'hpvz_se'    ,   400, -20,   20 )


hpvx_se.GetXaxis().SetTitle('PV x [cm]')
hpvy_se.GetXaxis().SetTitle('PV y [cm]')
hpvz_se.GetXaxis().SetTitle('PV z [cm]')

print 'histos created'

fillHistos(tree_pcl   , 
           hlumi_pcl  , 
           hnpv_pcl   ,
           hpvx_pcl   ,
           hpvy_pcl   ,
           hpvz_pcl      
           )

print 'filled 1'

fillHistos(tree_se    , 
           hlumi_se   , 
           hnpv_se    ,
           hpvx_se    ,
           hpvy_se    ,
           hpvz_se    
           )

print 'filled 2'

list_pcl.extend(( hpvx_pcl, hpvy_pcl, hpvz_pcl))
list_se.extend (( hpvx_se,  hpvy_se,  hpvz_se ))

for i in list_pcl:
  i.SetLineColor(2)
  

outfile = TFile.Open('histosPV_ReplayVsStreamExpress.root', 'recreate')
outfile.cd()
for i in list_pcl:
  i.Write()
for i in list_se:
  i.Write()
hnpv_pcl  .Write()
hnpv_se   .Write()
outfile.Close() 
   
