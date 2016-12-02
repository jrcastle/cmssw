import FWCore.ParameterSet.Config as cms

clusterCompatibilityFilter  = cms.EDFilter('HIClusterCompatibilityFilter',
                                           cluscomSrc           = cms.InputTag("hiClusterCompatibility"),
                                           minZ                 = cms.double(-20.0),
                                           maxZ                 = cms.double(20.05),
                                           clusterPars          = cms.vdouble(0.0,0.0045),
                                           nhitsTrunc           = cms.int32(150),
                                           clusterTrunc         = cms.double(2.0),
                                           pixelTune            = cms.untracked.bool(False),
                                           pixelTuneClusterPars = cms.untracked.vdouble(1.6116, 0.1602)
)
