import FWCore.ParameterSet.Config as cms

clusterCompatibilityFilter  = cms.EDFilter('HIClusterCompatibilityFilter',
                                           cluscomSrc               = cms.InputTag("hiClusterCompatibility"),
                                           minZ                     = cms.double(-20.0),
                                           maxZ                     = cms.double(20.05),
                                           clusterPars              = cms.vdouble(0.0,0.0045),
                                           nhitsTrunc               = cms.int32(150),
                                           clusterTrunc             = cms.double(2.0),
                                           pixelTune                = cms.untracked.bool(False),
                                           nhitsLineTrunc           = cms.untracked.int32(1000),
                                           pixelTuneLineClusterPars = cms.untracked.vdouble(2.14116, 0.000928176),
                                           pixelTunePolyClusterPars = cms.untracked.vdouble(3.01672, 5.63152e-05, -3.82712e-09, 1.31546e-13, -2.2803e-18, 1.57971e-23)
)
