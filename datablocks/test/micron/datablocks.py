import bisect
from dataclasses import dataclass
import importlib
from itertools import combinations
import gzip
import os
import pickle
from sklearn.cluster import KMeans
import tarfile
import tempfile
from typing import Optional, Dict, List

import fsspec
from fsspec.callbacks import TqdmCallback

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import fasttext
import matplotlib.pyplot as plt
import plotly.express as px
import umap

from micron.cclustering import ZSConsensusClustering


class Datablock:
    @staticmethod
    def display_umap(frame, *, color=None):
        _umap = umap.UMAP()
        _udata = _umap.fit_transform(frame.fillna(0.0))
        plt.scatter(_udata[:, 0], _udata[:, 1], c=color)

    def print_verbose(self, s):
        if self.verbose:
            print(f">>> {self.__class__.__qualname__}: {s}")

    def print_debug(self, s):
        if self.debug:
            print(f"DEBUG: >>> {self.__class__.__qualname__}: {s}")

    def path(self, roots, filesystem, topic=None):
        if topic is not None:
            if filesystem.protocol == 'file':
                if roots is None:
                    path = os.path.join(os.getcwd(), self.TOPICS[topic])
                else:
                    path_ = roots[topic]
                    os.makedirs(path_, exist_ok=True)
                    path = os.path.join(path_, self.TOPICS[topic])
            else:
                path = roots[topic] + "/" + self.TOPICS[topic]
        else:
            if filesystem.protocol == 'file':
                if roots is None:
                    path = os.join(os.getcwd(), self.FILENAME)
                else:
                    path_ = roots
                    os.makedirs(path_, exist_ok=True)
                    path = os.path.join(path_, self.FILENAME)
            else:
                path = roots + "/" + self.FILENAME
        return path

    #TODO: factor through 'valid()'
    def built(self, roots, filesystem):
        built = True
        if hasattr(self, 'TOPICS'):
            for topic in self.TOPICS:
                path = self.path(roots, filesystem, topic)
                if not filesystem.exists(path):
                    self.print_verbose(f"Topic '{topic}' not built at path {path}")
                    built = False
                    break
        else:
            path = self.path(roots, filesystem)
            built = filesystem.exists(path)
        return built

    def __init__(self, *, verbose=False, debug=False, rm_tmp=True, ):
        self.verbose = verbose
        self.debug = debug
        self.rm_tmp = rm_tmp


class miRCoHN(Datablock):
    """
        Data for the clustering HNSC study described in from https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7854517/.
        TODO: do not save 'pivots' or 'downregulated_mirna_infixes' to a file, return them from code instead?
    """
    VERSION = "0.11.3"
    
    TOPICS = {'logcounts': f"mircohn_rpm_log2.parquet",
                'pivots': f"mircohn_pivots.parquet",
                'logcontrols': f"mircohn_controls.parquet",
                'downregulated_mirna_infixes': f"mircohn_downregulated_mirna_infixes.parquet"
    }

    @dataclass
    class SCOPE:
        pass

    DOWNREGULATED_SEQ_PATTERNS = dict(
        epithelial = list(set(['miR-150', 'miR-125b', 'miR-195', 'miR-127', 'miR-342', 'miR-361',
                                  'miR-195', 'miR-125b', 'miR-150', 'miR-149', 'miR-342'
               
        ])),
        stromal = list(set(['miR-210', 'miR-20a', 'miR-92a', 'miR-20b', 'miR-17', 'miR-200c', 'miR-200b', 
                                   'miR-200a', 'miR-425', 'miR-18a', 'miR-183', 'miR-224', 'miR-181d', 'miR-221', 'miR-93', 'miR-106b', 
                                   'miR-194', 'miR-660',
                                   'miR-25', 'miR-106b', 'miR-93', 'miR-92a', 'miR-17', 'miR-20a', 'miR-210', 'miR-200a', 'miR-200c', 
                                   'miR-200b', 'miR-194'
        ]))
    )

    #https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7854517/bin/NIHMS1644540-supplement-3.docx
    PIVOT_SEQS = [
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

    #https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7854517/bin/NIHMS1644540-supplement-4.docx
    @staticmethod
    def control_records_mask(counts):
        controls = pd.Series(counts.index, index=counts.index).apply(lambda _: _.split('-')[3].startswith('11'))
        return controls

    _SRC_URL = "https://gdac.broadinstitute.org/runs/stddata__2016_01_28/data/HNSC/20160128/"
    _SRC_TAR_DIRNAME = "gdac.broadinstitute.org_HNSC.miRseq_Mature_Preprocess.Level_3.2016012800.0.0"
    _SRC_DAT_FILENAME = "HNSC.miRseq_mature_RPM_log2.txt"
    
    def display(
            self,
            roots: Optional[Dict[str, str]] = None,
            *, 
            filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"),
    ):
        cof = self.read(roots, filesystem=filesystem)
        self.display_umap(cof)
        """
        ufit = umap.UMAP()
        ucof = ufit.fit_transform(cof.fillna(0.0))
        plt.scatter(ucof[:, 0], ucof[:, 1])
        """

    @staticmethod
    def seq_matches_patterns(seq, seq_patterns): 
            for pattern in seq_patterns:
                if seq.find(pattern) != -1:
                    return True
            return False

    @staticmethod
    def filter_columns_by_patterns(frame, col_patterns):
        if col_patterns is not None:
            fcols = [col for col in frame.columns if miRCoHN.seq_matches_patterns(col, col_patterns)]
            fframe = frame[fcols]
        else:
            fframe = frame
        return fframe

    @staticmethod
    def filter_columns_by_mad(frame, mad_threshold):
        mads = (frame - frame.mean()).abs().mean()
        madf = pd.DataFrame({'mad': mads})
        madcuts = pd.qcut(madf.mad, 100, labels=False, duplicates='drop')
        madcols = madcuts[madcuts > mad_threshold].index
        madframe = frame[madcols]
        return madframe

    @staticmethod
    def center_at_controls(frame, controls):
        if controls is None:
            return frame
        cm = controls.mean(axis=0)
        cframe = frame - cm
        return cframe
    
    @staticmethod
    def display_heatmap(counts:             pd.DataFrame, 
                         *, 
                         ordering:           Optional[List[int]] = None, 
                         seq_patterns:       Optional[List[str]] = None, 
                         seq_mad_threshold:  float = 0.0,
                         center_at_controls: Optional[pd.DataFrame] = None,
                         nseqs:              Optional[int] = None,
    ):
        """
            Inputs/Output: see 'normalized_counts()'
        """
        ncounts1 = miRCoHN.filter_columns_by_patterns(counts, seq_patterns)
        if len(ncounts1.columns) == 0:
            raise ValueError(f"'filter_columns_by_patterns left no columns")
        ncounts2 = miRCoHN.filter_columns_by_mad(ncounts1, seq_mad_threshold)
        if center_at_controls is not None:
            ncounts3 = miRCoHN.center_at_controls(ncounts2, center_at_controls[ncounts2.columns])
        else:
            ncounts3 = ncounts2
        ncounts = ncounts3.iloc[ordering]

        ncountst_ = ncounts.transpose()
        if nseqs is not None:
            ncountst = ncountst_.iloc[:nseqs]
        else:
            ncountst = ncountst_
        _ = px.imshow(ncountst.values, aspect='auto')
        return _

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
        framepaths = {topic: self.path(roots, filesystem, topic) for topic in self.TOPICS}
        if self.built(roots, filesystem):
            self.print_verbose("All topics built already.  Done.")
        else:
            self.print_verbose("Building ...")
            # logcounts
            topic = 'logcounts'
            topic_tgt_path = framepaths[topic]
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
                logcounts_frame = pd.read_csv(logcounts_src_path, sep='\t', header=0, index_col=0).transpose()
                logcontrols_mask = miRCoHN.control_records_mask(logcounts_frame)
                topic_frame = _logcounts_frame = logcounts_frame[~logcontrols_mask]

                coltuples = [tuple(c.split('|')) for c in _logcounts_frame.columns]
                mindex = pd.MultiIndex.from_tuples(coltuples)
                _logcounts_frame.columns = mindex

                topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
                self.print_verbose(f"Wrote dataframe to {topic_tgt_path}")

            # pivots
            topic = 'pivots'
            topic_tgt_path = framepaths[topic]
            topic_frame = pivots_frame = pd.DataFrame({'pivots': self.PIVOT_SEQS})
            topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote dataframe to {topic_tgt_path}")

            #logcontrols
            topic = 'logcontrols'
            topic_tgt_path = framepaths[topic]
            topic_frame = logcontrols_frame = logcounts_frame[logcontrols_mask]
            ccoltuples = [tuple(c.split('|')) for c in logcontrols_frame.columns]
            cmindex = pd.MultiIndex.from_tuples(ccoltuples)
            logcontrols_frame.columns = cmindex
            topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote dataframe to {topic_tgt_path}")

            #downregulated
            topic = 'downregulated_mirna_infixes'
            topic_tgt_path = framepaths[topic]
            epithelial_downregulated_infixes = self.DOWNREGULATED_SEQ_PATTERNS['epithelial']
            stromal_downregulated_infixes = self.DOWNREGULATED_SEQ_PATTERNS['stromal']
            topic_frame = downregulated_frame = pd.DataFrame.from_records([{'epithelial': ','.join(list(epithelial_downregulated_infixes)), 
                                                                            'stromal': ','.join(list(stromal_downregulated_infixes))}])
            topic_frame.to_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote dataframe to {topic_tgt_path}")

            self.print_verbose("... done")
        return framepaths
    
    def read(self, 
             roots: Optional[Dict[str, str]] = None,
             *, 
             topic: str,
             filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"),  
    ):
        self.print_verbose(f"Reading topic '{topic}'")
        topic_tgt_path = self.path(roots, filesystem, topic)
        topic_frame = pd.read_parquet(topic_tgt_path, storage_options=filesystem.storage_options)
        return topic_frame


class miRNA(Datablock):
    VERSION = "0.2.1"

    @dataclass
    class SCOPE:
        pass

    MIRNA_DATASET_URL = "https://mirbase.org/download"
    MIRNA_DATASET_FILENAME = f"miRNA"
    FILENAME = f"{MIRNA_DATASET_FILENAME}.parquet"
    
    def build(self,
              root,
              *,
              scope: SCOPE = SCOPE(),
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file")):
        if self.built(root, filesystem):
            self.print_verbose(f"miRNA already built in root {root}")
        else:
            self.print_verbose(f"Building miRNA ...")

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

            path = self.path(root, filesystem)
            frame.to_parquet(path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote frame of len {len(frame)} to path")
            self.print_verbose("... done")

    def read(self,
              root,
              *,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file")
    ):
        path = self.path(root, filesystem)
        frame = pd.read_parquet(path, storage_options=filesystem.storage_options)
        return frame
    
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


class miRCoSeqs(Datablock):
    """
        Sequences sampled at count frequences
    """
    VERSION = "1.2.0"
    TOPICS = {'logcounts': f"miRLogCos.parquet",
                 'counts': f"miRCos.parquet",
                 'logcontrols': f"miRLogCtrls.parquet",
                 'seqs': f"miRSeqs.parquet",
                 'samples': f"miRCoSeqs.txt",
                 'rec_sample_ranges': f"miRSampleRanges.parquet"
    }
    
    @dataclass
    class SCOPE:
        seqs: pd.DataFrame
        logcounts: pd.DataFrame
        logcontrols: pd.DataFrame
        npasses: int = 5
        nseqs_per_record: int = 200    

    @staticmethod
    def count_cols_to_coseq_cols(columns):
        subcolumns = {c: c[1][5:] for c in columns}
        return subcolumns

    @staticmethod
    def expcounts(logcounts):
        counts = np.exp(logcounts.copy()*np.log(2))
        return counts
    
    def build(self,
              roots: Dict[str, str],
              *,
              scope: SCOPE,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file")):

        def lift_jcols(frame, coseq2co):
            _cocols = frame.rename(columns=coseq2co).columns
            cocols = pd.MultiIndex.from_tuples(_cocols)
            frame.columns = cocols
            return frame

        if self.built(roots, filesystem):
            self.print_verbose(f"miRCoSeqs already built")
        else:
            self.print_verbose(f"Building miRCoSeqs ... ")
            logcof = scope.logcounts
            co2coseq = self.count_cols_to_coseq_cols(logcof.columns)
            coseq2co = {val: key for key, val in co2coseq.items()}
            _logcof = logcof.copy()
            _logcof.columns = list(co2coseq.values())

            # log2(n) -> n
            # n = exp(ln(n)) = exp[ln(2^log2(n))] = exp[log2(n)*ln(2)]
            _cof = self.expcounts(_logcof)

            seqf = scope.seqs
            accession = seqf.Accession.apply(lambda _: _[2:])
            _seqf = seqf.copy()
            _seqf['accession'] = accession
            _seqf.set_index('accession', inplace=True)

            # join columns/datasets
            jcols = [i for i in _seqf.index if i in _cof.columns]

            jcof = lift_jcols(_cof[jcols], coseq2co)
            jcof_path = self.path(roots, filesystem, 'counts')
            jcof.to_parquet(jcof_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote counts to {jcof_path}")

            jlogcof = lift_jcols(_logcof[jcols], coseq2co)
            jlogcof_path = self.path(roots, filesystem, 'logcounts')
            jlogcof.to_parquet(jlogcof_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote logcounts to {jlogcof_path}")
            
            jseqf = lift_jcols(_seqf.loc[jcols].transpose(), coseq2co).transpose()
            jseqs_path = self.path(roots, filesystem, 'seqs')
            jseqf.to_parquet(jseqs_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote sequences to {jseqs_path}")

            jcof0 = jcof.fillna(0.0)
            jcofn = jcof0.div(jcof0.sum(axis=1), axis=0)
            jseqs = jseqf['sequence']
            jseqlist = jseqs.tolist()

            jlogcontrols = scope.logcontrols[co2coseq.keys()]
            jlogcontrols_path = self.path(roots, filesystem, 'logcontrols')
            jlogcontrols.to_parquet(jlogcontrols_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote logcontrols to {jlogcontrols_path}")

            self.print_verbose(f"Generating samples using {scope.npasses} passes")
            rec_sample_ranges = {recidx: [] for recidx in jcofn.index}
            sample_batches = []
            n_samples = 0
            rng = np.random.default_rng()
            for _pass in range(scope.npasses):
                self.print_verbose(f"pass {_pass}")
                n_pass_samples = 0
                for recidx, rec in jcofn.iterrows():
                    rec_sample_batches = []
                    n_rec_samples = 0
                    seqfreqs = rng.multinomial(scope.nseqs_per_record, rec)
                    for i, freq in enumerate(seqfreqs):
                        if freq == 0: continue
                        rec_sample_batches.append((i, freq))#jseqlist[i:i+1]*freq
                        n_rec_samples += freq
                    rec_sample_ranges[recidx].append((n_samples, n_samples + n_rec_samples))
                    sample_batches += rec_sample_batches
                    n_samples += n_rec_samples
                    n_pass_samples += n_rec_samples
                self.print_verbose(f"Generated {n_pass_samples} in pass {_pass} for a total of {n_samples} so far")
            self.print_verbose(f"Generated {n_samples} samples")

            samples_path = self.path(roots, filesystem, 'samples')
            with filesystem.open(samples_path, 'w') as f:
                self.print_verbose(f"Writing {n_samples} samples to {samples_path}")
                for i, freq in sample_batches:
                    batch = jseqlist[i:i+1]*freq
                    s = "\n".join(batch)+"\n"
                    f.write(s)

            rec_sample_ranges_frame = pd.DataFrame(rec_sample_ranges).transpose()
            rec_sample_ranges_frame.columns.name = 'pass'
            rec_sample_ranges_path = self.path(roots, filesystem, 'rec_sample_ranges')
            rec_sample_ranges_frame.to_parquet(rec_sample_ranges_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote {len(rec_sample_ranges_frame)} to {rec_sample_ranges_path}")
        self.print_verbose("... done")

    def read(self,
             roots,
             *,
             topic,
             filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"),
    ):
        path = self.path(roots, filesystem, topic)
        if topic == 'samples':
            with filesystem.open(path, 'r') as f:
                s = f.read()
            self.print_debug(f"Read string of len {len(s)} from {path}")
            useqs = s.split('\n')
            self.print_verbose(f"Read {len(useqs)} useqs")
            _ = useqs
        elif topic in self.TOPICS:
            _ = pd.read_parquet(path, storage_options=filesystem.storage_options)
        else:
            raise ValueError(f"Unknown topic: {topic}")
        return _
    
    def display(self,
                roots,
                *,
                topic,
                filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"),
    ):
        if topic == 'samples':
            ...
        elif topic == 'logcounts': 
            ...
        elif topic == 'counts': 
            ...
        elif topic == 'seqs':
            ...


class ZSCC(Datablock):
    VERSION = "0.6.1"
    TOPICS = {
        'zscc': 'zscc.pkl',
        'clusters': 'clusters.parquet',
        'ordering': 'ordering.pkl'
    }
    
    @dataclass
    class SCOPE:
        """
            * `clustering` is a Callable that takes `n_clusters: int`
            * `clustering(n).fit_predict(y: np.array(m, n)) -> np.array(m)` returns cluster assignments
        """
        data_frame: pd.DataFrame
        clustering: str = 'sklearn.cluster.KMeans'
        n_reps: int = 100
        lo: int = 2
        hi: int = 5
        fillna: Optional[float] = None

    def build(self,
              roots,
              *,
              scope: SCOPE,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file")):
        if self.built(roots, filesystem):
            self.print_verbose(f"ZSCC already built")
        else:
            self.print_verbose("Building ZSCC ...")
            clparts = scope.clustering.split('.')
            clmodname = '.'.join(clparts[:-1])
            clclassname = clparts[-1]
            clmod = importlib.import_module(clmodname)
            clustering = getattr(clmod, clclassname)

            zscc = ZSConsensusClustering(clustering, n_reps=scope.n_reps, lo=scope.lo, hi=scope.hi)
            data_frame = scope.data_frame if scope.fillna is None else scope.data_frame.fillna(scope.fillna)
            self.print_verbose(f"Fitting zscc to data of size {len(data_frame)}")
            zscc.fit(data_frame.values)

            zscc_path = self.path(roots, filesystem, 'zscc')
            with filesystem.open(zscc_path, 'wb') as zsccfile:
                pickle.dump(zscc, zsccfile)
            if self.verbose:
                print(f"Wrote zscc pickle to {zscc_path}")

            if self.verbose:
                print(f"Assigning optimal clusters to data of len {len(data_frame)}")
            clusters_path = self.path(roots, filesystem, 'clusters')
            clusters = zscc.predict_data(data_frame.values)
            clusters_frame = pd.DataFrame({'clusters': clusters})
            clusters_frame.to_parquet(clusters_path, storage_options=filesystem.storage_options)
            self.print_verbose(f"Wrote zscc clusters to {clusters_path}")

            ordering_path = self.path(roots, filesystem, 'ordering')
            ordering = np.argsort(clusters)
            with filesystem.open(ordering_path, 'wb') as ordering_file:
                pickle.dump(ordering, ordering_file)
            self.print_verbose(f"Wrote zscc ordering to {ordering_path}")
        self.print_verbose("... done")

    def read(self,
              roots,
              *,
              topic,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file"), 
    ):
    
        if topic == 'zscc':
            zscc_path = self.path(roots, filesystem, 'zscc')
            with filesystem.open(zscc_path, 'rb') as zsccfile:
                zscc = pickle.load(zsccfile)
                _ = zscc
            if self.verbose:
                print(f"Loaded zscc pickle from {zscc_path}")
        elif topic == 'clusters':
            clusters_path = self.path(roots, filesystem, 'clusters')
            clusters_frame = pd.read_parquet(clusters_path, storage_options=filesystem.storage_options)
            _ = clusters_frame
            if self.verbose:
                print(f"Read zscc clusters from {clusters_path}")
        elif topic == 'ordering':
            ordering_path = self.path(roots, filesystem, 'ordering')
            with filesystem.open(ordering_path, 'rb') as ordering_file:
                _ = pickle.load(ordering_file)
            if self.verbose:
                print(f"Read zscc cluster ordering from {ordering_path}")
        else:
            raise ValueError(f"Unknown topic '{topic}'")
        return _


class FastText(Datablock):
    VERSION = "0.6.1"
    
    @dataclass
    class SCOPE:
        '''
        fasttext manpage:
            input             # training file path (required)
            model             # unsupervised fasttext model {cbow, skipgram} [skipgram]
            lr                # learning rate [0.05]
            dim               # size of word vectors [100]
            ws                # size of the context window [5]
            epoch             # number of epochs [5]
            minCount          # minimal number of word occurences [5]
            minn              # min length of char ngram [3]
            maxn              # max length of char ngram [6]
            neg               # number of negatives sampled [5]
            wordNgrams        # max length of word ngram [1]
            loss              # loss function {ns, hs, softmax, ova} [ns]
            bucket            # number of buckets [2000000]
            thread            # number of threads [number of cpus]
            lrUpdateRate      # change the rate of updates for the learning rate [100]
            t                 # sampling threshold [0.0001]
            verbose           # verbose [2]
        '''
        samples_path: str
        model: str = "cbow"
        dim: int = 100
        context_window_size: int = 100

    FILENAME = "model.bin"
    
    def build(self,
              root,
              *,
              scope: SCOPE,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file")):
        if self.built(root, filesystem):
            self.print_verbose(f"'{scope.model}' already built")
        else:
            self.print_verbose(f"Building '{scope.model}' ...")
            path = self.path(root, filesystem)
            if not filesystem.exists(path):
                samples_files = filesystem.ls(scope.samples_path)
                assert len(samples_files) == 1
                samples_path = samples_files[0]
                cbow = fasttext.train_unsupervised(samples_path, model=scope.model, dim=scope.dim, ws=scope.context_window_size)
                cbow.save_model(path)
            self.print_verbose("... done")

    def read(self,
              root,
              *,
              filesystem: fsspec.AbstractFileSystem = fsspec.filesystem("file")):
        path = self.path(root, filesystem)
        model = fasttext.load_model(path)
        return model
