from dataclasses import dataclass
import os
import tarfile
import tempfile
from typing import Optional, Dict

import fsspec
from fsspec.callbacks import TqdmCallback

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import matplotlib.pyplot as plt
import umap

import ray


DATALAKE = os.path.join(os.environ['HOME'], '.cache', 'datalake', 'micron', 'dataset')


class Dataset:
    def print_verbose(self, s):
        if self.verbose:
            print(f">>> {self.__class__.__qualname__}: {s}")

    def print_debug(self, s):
        if self.debug:
            print(f"DEBUG: >>> {self.__class__.__qualname__}: {s}")


class miRCoHN(Dataset):
    """
        Data for the clustering HNSC study described in from https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7854517/.
    """
    VERSION = "0.5.3"
    TOPICS            = ['logcounts', 
                         'pivots',
                         'controls',
                         'downregulated_mirna_infixes']
    @dataclass
    class SCOPE:
        pass

    _TGT_FILENAMES = {'logcounts': f"mircohn_rpm_log2.parquet",
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

    def display(
             roots: Optional[Dict[str, str]] = None,
             *, 
             scope: Optional[SCOPE] = None, 
             filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"),
    ):
        cof = self.read(roots, scope=scope, filesystem=filesystem)
        ufit = umap.UMAP()
        ucof = ufit.fit_transform(cof.fillna(0.0))
        plt.scatter(ucof[:, 0], ucof[:, 1])

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
            self.print_verbose(f"Resetting filesystem to {filesystem} because None 'roots' default to 'os.getcwd()'")

        self.print_verbose("Building ...")

        framepaths = {}

        # logcounts
        topic = 'logcounts'
        fs = fsspec.filesystem('http')
        with tempfile.TemporaryDirectory() as tmpdir:
            remote_tarpath = self._SRC_URL + '/' + self._SRC_TAR_DIRNAME + ".tar.gz"
            local_tarpath = os.path.join(tmpdir, self._SRC_TAR_DIRNAME) + ".tar.gz"
            self.print_verbose(f"Downloading {remote_tarpath} to {local_tarpath}")
            fs.get(remote_tarpath, local_tarpath, callback=TqdmCallback())
            assert os.path.isfile(local_tarpath)
            self.print_verbose(f"Trying to parse local copy {local_tarpath}")
            _tardir = os.path.join(tmpdir, self._SRC_TAR_DIRNAME)
            with tarfile.open(local_tarpath, 'r') as _tarfile:
                self.print_verbose(f"Extracting {local_tarpath} to {_tardir}")
                _tarfile.extractall(tmpdir)
            self.print_debug(f"Extracted dir: {os.listdir(_tardir)}")
            logcounts_src_path = os.path.join(_tardir, self._SRC_DAT_FILENAME)
            topic_frame = logcounts_frame = pd.read_csv(logcounts_src_path, sep='\t', header=0, index_col=0).transpose()

            coltuples = [tuple(c.split('|')) for c in logcounts_frame.columns]
            mindex = pd.MultiIndex.from_tuples(coltuples)
            logcounts_frame.columns = mindex

            topic_tgt_root = roots[topic] if roots is not None else os.getcwd()
            filesystem.mkdirs(topic_tgt_root, exist_ok=True)
            topic_tgt_path = os.path.join(topic_tgt_root, self._TGT_FILENAMES[topic])
            topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote dataframe to {topic_tgt_path}")
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
        self.print_verbose(f"Wrote dataframe to {topic_tgt_path}")
        framepaths[topic] = topic_tgt_path

        #controls
        topic = 'controls'
        controls = pd.Series(logcounts_frame.index, index=logcounts_frame.index).apply(lambda _: _.split('-')[3].startswith('11'))
        controls.name = 'controls'
        topic_frame = controlsf = pd.DataFrame({'is_control': controls})

        topic_tgt_root = roots[topic] if roots is not None else os.getcwd()
        filesystem.makedirs(topic_tgt_root, exist_ok=True)
        topic_tgt_path = os.path.join(topic_tgt_root, self._TGT_FILENAMES[topic])
        topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
        self.print_verbose(f"Wrote dataframe to {topic_tgt_path}")
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
        self.print_verbose(f"Wrote dataframe to {topic_tgt_path}")
        framepaths[topic] = topic_tgt_path

        #https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7854517/bin/NIHMS1644540-supplement-4.docx
        self.print_verbose("... done")
        return framepaths
    
    def read(self, 
             roots: Optional[Dict[str, str]] = None,
             *, 
             topic: str,
             filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"),  
    ):
        self.print_verbose(f"Reading topic '{topic}'")
        topic_tgt_root = roots[topic] if roots is not None else os.getcwd()
        topic_tgt_path = os.path.join(topic_tgt_root, self._TGT_FILENAMES[topic])
        topic_frame = pd.read_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
        return topic_frame
        
    
class miRCoStats(Dataset):
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
        
        self.print_verbose("Building miRCo stats")

        mc = scope.mirco
        mcmad = (mc - mc.mean()).abs().mean().sort_values(ascending=False)
        mcmadf = pd.DataFrame({'mad': mcmad})
        mcmadf_path = root + "/" + self.TGT_FILENAME      
        mcmadf.to_parquet(mcmadf_path, storage_options=filesystem.storage_options)
        self.print_verbose(f"Wrote dataframe to {mcmadf_path}")
        return mcmadf_path
    
    def read(self, 
             root: Optional[str] = None,
             *, 
             filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"), 
    ):
        mcmadf_root = root or os.getcwd()
        mcmadf_path = os.path.join(mcmadf_root, self.TGT_FILENAME)
        mcmadf_frame = pd.read_parquet(mcmadf_path, storage_options=filesystem.storage_options)
        return mcmadf_frame
    

class miRNA(Dataset):
    VERSION = "0.0.1"

    @dataclass
    class SCOPE:
        pass

    MIRNA_DATASET_URL = "https://mirbase.org/download"
    MIRNA_DATASET_FILENAME = f"miRNA"

    def __init__(self, verbose=False, debug=False, rm_tmp=True, ):
        self.verbose = verbose
        self.debug = debug
        self.rm_tmp = rm_tmp
    
    def build(self,
              root,
              *,
              scope: SCOPE = SCOPE(),
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file")):
        
        root = root or os.getcwd()
        fs = fsspec.filesystem('http')

        remote_dat = self.MIRNA_DATASET_URL + '/' + f'{self.MIRNA_DATASET_FILENAME}.dat'
        local_dat = os.path.join(root, f'{self.MIRNA_DATASET_FILENAME}.dat')
        if not os.path.isfile(local_dat):
            self.print_verbose(f"Downloading {remote_dat} to {local_dat}")
            fs.get(remote_dat, local_dat, callback=TqdmCallback())
        self.print_verbose(f"Parsing local copy {local_dat}")

        if local_dat.endswith('.gz'):
            with gzip.open(local_dat, 'r') as datfile:
                datstr = datfile.read().decode()
                frame = self._build_frame(datstr)
        else:
            with open(local_dat, 'r') as datfile:
                datstr = datfile.read()
                frame = self._build_frame(datstr)

        path = self.path(root)
        frame.to_parquet(path, storage_options=filesystem.storage_options)

    def read(self,
              root,
              *,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file")
    ):
        root = root or os.getcwd()
        path = self.path(root)
        frame = pd.read_parquet(path, storage_options=filesystem.storage_options)
        return frame

    def path(self, root):
        path = os.path.join(root, f"{self.MIRNA_DATASET_FILENAME}.parquet",) 
        return path

    
    def _build_frame(self, mdstr):
        recs = miRNA._parse_records(mdstr)
        f = pd.DataFrame.from_records(recs)
        frame = f.sort_values('ID').reset_index(drop=True)
        self.print_verbose(f"Built dataframe")
        return frame

    @staticmethod     
    def _parse_records(mdstr):
        _mdstrs = mdstr.split('\nID')
        mdstrs = [f"ID{s}" for s in _mdstrs]
        _prerecs = [miRNA._parse_prerecord(s) for s in mdstrs]
        prerecs = [pr for pr in _prerecs if pr['DE'].find('sapiens') != -1]
        recs = [miRNA._prerecord_to_record(pr) for pr in prerecs]
        return recs

    @staticmethod
    def _parse_prerecord(recstr):
        sqstart = recstr.find('SQ')+2
        sqend = recstr.find('//')
        sq = recstr[sqstart:sqend]
        recstrs = recstr.split('\n')
        rec_ = {s[:2]: s[3:] for s in recstrs}
        _rec = {k: v.strip() for k, v in rec_.items() if k in ['ID', 'AC', 'DE']}
        _rec['SQ'] = sq
        return _rec

    @staticmethod
    def _prerecord_to_record(prerec):
        rec = {}
        _id = prerec['ID'].split(' ')[0]
        rec['ID'] = _id
        _ac = prerec['AC']
        rec['Accession'] = _ac[:-1] if _ac[-1] == ';' else _ac
        sq_ = prerec['SQ']
        sq_strs_ = sq_.split('\n')[1:-1]
        _sq = ''.join([s[:-2].strip() for s in sq_strs_])
        sq = ''.join([s.strip() for s in _sq.split(' ')])
        rec['sequence'] = ''.join([c for c in sq.upper() if c in ['A', 'C', 'G', 'U']])
        return rec


class miRCoSeqs(Dataset):
    """
        Sequences sampled at count frequences
    """
    VERSION = "0.2.1"
    TOPICS = ['counts', 'seqs', 'samples']
    
    @dataclass
    class SCOPE:
        seqs: pd.DataFrame
        logcounts: pd.DataFrame
        nepochs: int = 5
        nsamples_per_record: int = 200
        npermutations: int = 1

    MIRCOSEQS_COUNTS_FILENAME = f"miRCos.txt"
    MIRCOSEQS_SEQS_FILENAME = f"miRSeqs.parquet"
    MIRCOSEQS_SAMPLES_FILENAME = f"miRCoSeqs.parquet"
    FILENAMES = {'counts': MIRCOSEQS_COUNTS_FILENAME,
                 'seqs': MIRCOSEQS_SEQS_FILENAME,
                 'samples': MIRCOSEQS_SAMPLES_FILENAME,
    }

    def __init__(self, verbose=False, debug=False, rm_tmp=True, ):
        self.verbose = verbose
        self.debug = debug
        self.rm_tmp = rm_tmp
    
    def build(self,
              roots: Dict[str, str],
              *,
              scope: SCOPE,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file")):
        
        # log2(n) -> n
        # n = exp(ln(n)) = exp[ln(2^log2(n))] = exp[log2(n)*ln(2)]
        logcof = scope.logcounts
        logcofc1 = [c[5:] for c in logcof.columns.get_level_values(1).tolist()]
        _acof = np.exp(logcof.copy()*np.log(2))
        _acof.columns = logcofc1

        seqf = scope.seqs
        accession = seqf.Accession.apply(lambda _: _[2:])
        _seqf = seqf.copy()
        _seqf['accession'] = accession
        _aseqf = _seqf.set_index('accession')

        acols = [i for i in _aseqf.index if i in _alogcof.columns]
        acof = _acof[acols]
        acof_path = self.path(roots, 'counts')
        acof.to_parquet(acof_path, storage_options=filesystem.storage_options)
        self.print_verbose(f"Wrote counts to {acof_path}")

        acof0 = acof[acols].fillna(0.0)
        acof1 = acof0.div(acof0.sum(axis=1), axis=0)

        rng = np.random.default_rng()
        
        aseqf = _aseqf.loc[acols]
        aseqs_path = self.path(roots, 'seqs')
        aseqf.to_parquet(aseqs_path, storage_options=filesystem.storage_options)
        self.print_verbose(f"Wrote sequences to {aseqs_path}")

        aseqs = _aseqf.loc[acols, 'sequence']
        aseqlist = aseqs.tolist()
        rng = np.random.default_rng()
        _samples = []
        self.print_verbose(f"Generating samples from {scope.nepochs} epochs")
        for epoch in range(scope.nepochs):
            self.print_verbose(f"epoch {epoch}")
            for _, rec in acof1.iterrows():
                _samplecounts = rng.multinomial(scope.nsamples_per_record, rec)
                for i, c in enumerate(_samplecounts):
                    if c == 0: continue
                    _samples += aseqlist[i:i+1]*c
        self.print_verbose(f"Generated {len(_samples)} samples")
        samples_ = np.array(_samples)
        perm = rng.permutation(len(samples_))
        samples = samples_
        self.print_verbose(f"Randomizing {len(_samples)} samples using {scope.npermutations} permutations")
        for i in range(scope.npermutations):
            samples = samples[perm]

        samples_path = self.path(roots, 'samples')
        with filesystem.open(samples_path, 'w') as f:
            self.print_verbose(f"Writing {len(samples)} to {samples_path}")
            self.print_debug(f"samples[:10]:\n{samples[:10]}")
            s = "\n".join(samples)
            f.write(s)

    def read(self,
              roots,
              *,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"),
              topic,
    ):
        path = self.path(roots, topic)
        if topic == 'samples':
            with filesystem.open(path, 'r') as f:
                s = f.read()
            self.print_debug(f"Read string of len {len(s)} from {path}")
            useqs = s.split('\n')
            self.print_verbose(f"Read {len(useqs)} useqs")
            _ = useqs
        elif topic == 'counts': 
            _ = pd.read_parquet(path, storage_options=filesystem.storage_options)
        elif topic == 'seqs':
            _ = pd.read_parquet(path, storage_options=filesystem.storage_options)
        return _
    
    def valid(self,
              roots,
              *,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"),
              topic,
    ):
        path = self.path(roots, topic)
        _ = filesystem.exists(path)
        return _

    def path(self, roots, topic):
        if topic not in self.TOPICS: 
            raise ValueError(f"Topic {topic} not in {self.TOPICS}")
        filename = self.FILENAMES[topic]
        root = roots[topic] if roots else os.getcwd()
        path = os.path.join(root, filename,) 
        return path

    

    

    