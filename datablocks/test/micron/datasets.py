from dataclasses import dataclass
import tarfile
import tempfile
from typing import Optional, Dict

import os

import fsspec
from fsspec.callbacks import TqdmCallback


import pandas as pd
import pyarrow.parquet as pq

import ray


DATALAKE = os.path.join(os.environ['HOME'], '.cache', 'datalake', 'micron', 'dataset')


class miRCoHN:
    """
        Data for the clustering HNSC study described in from https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7854517/.
    """
    VERSION = "0.4.3"
    TOPICS            = ['counts', 
                         'pivots',
                         'controls',
                         'downregulated_mirna_infixes']
    @dataclass
    class SCOPE:
        pass

    _TGT_FILENAMES = {'counts': f"mircohn_rpm_log2.parquet",
                      'pivots': f"mircohn_pivots.parquet",
                      'controls': f"mircohn_controls.parquet",
                      'downregulated_mirna_infixes': f"mircohn_downregulated_mirna_infixes.parquet"
    }

    _SRC_URL = "https://gdac.broadinstitute.org/runs/stddata__2016_01_28/data/HNSC/20160128/"
    _SRC_TAR_DIRNAME = "gdac.broadinstitute.org_HNSC.miRseq_Mature_Preprocess.Level_3.2016012800.0.0"
    _SRC_DAT_FILENAME = "HNSC.miRseq_mature_RPM_log2.txt"
    
    def __init__(self, *, debug=False, verbose=False):
        self.debug = debug
        self.verbose = verbose

    def build(self, 
             roots: Optional[Dict[str, str]] = None,
             *, 
             scope: Optional[SCOPE] = None, 
             filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"), 
    ):
        """
            Generate a pandas dataframe of TCGA HNSC mature MiRNA sequence samples.
        """
        scope = scope or self.SCOPE()
        if roots is None and filesystem.protocol != 'file':
            filesystem = fsspec.AbstractFileSystem = fsspec.filesystem("file")
            if self.verbose:
                print(f"Resetting filesystem to {filesystem} because None 'roots' default to 'os.getcwd()'")

        if self.verbose:
            print(">>> Building miRCoHN")

        framepaths = {}

        # counts
        topic = 'counts'
        fs = fsspec.filesystem('http')
        with tempfile.TemporaryDirectory() as tmpdir:
            remote_tarpath = self._SRC_URL + '/' + self._SRC_TAR_DIRNAME + ".tar.gz"
            local_tarpath = os.path.join(tmpdir, self._SRC_TAR_DIRNAME) + ".tar.gz"
            if self.verbose:
                print(f"Downloading {remote_tarpath} to {local_tarpath}")
            fs.get(remote_tarpath, local_tarpath, callback=TqdmCallback())
            assert os.path.isfile(local_tarpath)
            if self.verbose:
                print(f"Trying to parse local copy {local_tarpath}")
            _tardir = os.path.join(tmpdir, self._SRC_TAR_DIRNAME)
            with tarfile.open(local_tarpath, 'r') as _tarfile:
                if self.verbose:
                    print(f"Extracting {local_tarpath} to {_tardir}")
                _tarfile.extractall(tmpdir)
            if self.debug:
                print(f"DEBUG: extracted dir: {os.listdir(_tardir)}")
            counts_src_path = os.path.join(_tardir, self._SRC_DAT_FILENAME)
            topic_frame = counts_frame = pd.read_csv(counts_src_path, sep='\t', header=0, index_col=0).transpose()

            coltuples = [tuple(c.split('|')) for c in counts_frame.columns]
            mindex = pd.MultiIndex.from_tuples(coltuples)
            counts_frame.columns = mindex

            topic_tgt_root = roots[topic] if roots is not None else os.getcwd()
            filesystem.mkdirs(topic_tgt_root, exist_ok=True)
            topic_tgt_path = os.path.join(topic_tgt_root, self._TGT_FILENAMES[topic])
            topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
            if self.verbose:
                print(f"Wrote dataframe to {topic_tgt_path}")
            framepaths[topic] = topic_tgt_path

        # pivots
        topic = 'pivots'
        #https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7854517/bin/NIHMS1644540-supplement-3.docx
        pivot_list = [
            "hsa-let-7d-5p",
            "hsa-miR-103a-3p",
            "hsa-miR-106a-5p",
            "hsa-miR-106b-3p",
            "hsa-miR-106b-5p",
            "hsa-miR-1180-3p",
            "hsa-miR-125b-5p",
            "hsa-miR-127-5p",
            "hsa-miR-1301-3p",
            "hsa-miR-1307-3p",
            "hsa-miR-149-5p",
            "hsa-miR-150-5p",
            "hsa-miR-151a-5p",
            "hsa-miR-17-3p",
            "hsa-miR-17-5p",
            "hsa-miR-181d-5p",
            "hsa-miR-182-5p",
            "hsa-miR-183-5p",
            "hsa-miR-18a-3p",
            "hsa-miR-194-5p",
            "hsa-miR-195-5p",
            "hsa-miR-200a-3p",
            "hsa-miR-200a-5p",
            "hsa-miR-200b-3p",
            "hsa-miR-200b-5p",
            "hsa-miR-200c-3p",
            "hsa-miR-205-5p",
            "hsa-miR-20a-5p",
            "hsa-miR-20b-5p",
            "hsa-miR-210-3p",
            "hsa-miR-221-3p",
            "hsa-miR-222-3p",
            "hsa-miR-224-5p",
            "hsa-miR-23b-5p",
            "hsa-miR-25-3p",
            "hsa-miR-27a-5p",
            "hsa-miR-27b-5p",
            "hsa-miR-320c",
            "hsa-miR-324-3p",
            "hsa-miR-342-5p",
            "hsa-miR-361-5p",
            "hsa-miR-369-5p",
            "hsa-miR-423-3p",
            "hsa-miR-425-5p",
            "hsa-miR-493-3p",
            "hsa-miR-660-5p",
            "hsa-miR-769-3p",
            "hsa-miR-92a-3p",
            "hsa-miR-93-3p",
            "hsa-miR-93-5p",
        ]
        topic_frame = pivots_frame = pd.DataFrame({'pivots': pivot_list})
        topic_tgt_root = roots[topic] if roots is not None else os.getcwd()
        filesystem.makedirs(topic_tgt_root, exist_ok=True)
        topic_tgt_path = os.path.join(topic_tgt_root, self._TGT_FILENAMES[topic])
        topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
        if self.verbose:
            print(f"Wrote dataframe to {topic_tgt_path}")
        framepaths[topic] = topic_tgt_path

        #controls
        topic = 'controls'
        controls = pd.Series(counts_frame.index, index=counts_frame.index).apply(lambda _: _.split('-')[3].startswith('11'))
        controls.name = 'controls'
        topic_frame = controlsf = pd.DataFrame({'is_control': controls})

        topic_tgt_root = roots[topic] if roots is not None else os.getcwd()
        filesystem.makedirs(topic_tgt_root, exist_ok=True)
        topic_tgt_path = os.path.join(topic_tgt_root, self._TGT_FILENAMES[topic])
        topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
        if self.verbose:
            print(f"Wrote dataframe to {topic_tgt_path}")
        framepaths[topic] = topic_tgt_path

        #downregulated
        topic = 'downregulated_mirna_infixes'
        epithelial_downregulated_infixes = set(['miR-150', 'miR-125b', 'miR-195', 'miR-127', 'miR-342', 'miR-361',
                                  'miR-195', 'miR-125b', 'miR-150', 'miR-149', 'miR-342'
               
        ])
        stromal_downregulated_infixes = set(['miR-210', 'miR-20a', 'miR-92a', 'miR-20b', 'miR-17', 'miR-200c', 'miR-200b', 
                                   'miR-200a', 'miR-425', 'miR-18a', 'miR-183', 'miR-224', 'miR-181d', 'miR-221', 'miR-93', 'miR-106b', 
                                   'miR-194', 'miR-660',
                                   'miR-25', 'miR-106b', 'miR-93', 'miR-92a', 'miR-17', 'miR-20a', 'miR-210', 'miR-200a', 'miR-200c', 
                                   'miR-200b', 'miR-194'
        ])
        topic_frame = downregulated_frame = pd.DataFrame.from_records([{'epithelial': ','.join(list(epithelial_downregulated_infixes)), 
                                                                        'stromal': ','.join(list(stromal_downregulated_infixes))}])
        topic_tgt_root = roots[topic] if roots is not None else os.getcwd()
        filesystem.makedirs(topic_tgt_root, exist_ok=True)
        topic_tgt_path = os.path.join(topic_tgt_root, self._TGT_FILENAMES[topic])
        topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
        if self.verbose:
            print(f">>> Wrote dataframe to {topic_tgt_path}")
        framepaths[topic] = topic_tgt_path

        #https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7854517/bin/NIHMS1644540-supplement-4.docx
        return framepaths
    
    def read(self, 
             roots: Optional[Dict[str, str]] = None,
             *, 
             topic: str,
             scope: Optional[SCOPE] = None, 
             filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"),  
    ):
        if self.verbose:
            print(f">>> Reading miRCoHN topic {topic}")
        topic_tgt_root = roots[topic] if roots is not None else os.getcwd()
        topic_tgt_path = os.path.join(topic_tgt_root, self._TGT_FILENAMES[topic])
        topic_frame = pd.read_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
        return topic_frame
        
    
class miRCoStats:
    """
        MAD
    """
    VERSION = "0.3.1"

    TGT_FILENAME = f"mirco_stats.parquet"

    @dataclass
    class SCOPE:
        mirco: pd.DataFrame

    def __init__(self, *, verbose=False, debug=False):
        self.verbose = verbose
        self.debug = debug

    def build(self, 
             root: Optional[str] = None,
             *, 
             scope: SCOPE, 
             filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"), 
    ):
        """
            Generate a pandas dataframe of miRCo statistics.
        """
        root = root or os.getcwd()
        
        if self.verbose:
            print(">>> Building miRCo stats")

        mc = scope.mirco
        mcmad = (mc - mc.mean()).abs().mean().sort_values(ascending=False)
        mcmadf = pd.DataFrame({'mad': mcmad})
        mcmadf_path = root + "/" + self.TGT_FILENAME      
        mcmadf.to_parquet(mcmadf_path, storage_options=filesystem.storage_options)
        if self.verbose:
            print(f">>> Wrote dataframe to {mcmadf_path}")
        return mcmadf_path
    
    def read(self, 
             root: Optional[str] = None,
             *, 
             scope: Optional[SCOPE] = None, 
             filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"), 
    ):
        mcmadf_root = root or os.getcwd()
        mcmadf_path = os.path.join(mcmadf_root, self.TGT_FILENAME)
        mcmadf_frame = pd.read_parquet(mcmadf_path, storage_options=filesystem.storage_options)
        return mcmadf_frame