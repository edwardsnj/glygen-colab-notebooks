"""
GlyGen Data Utilities

This module provides utilities to discover, download, cache, and process 
datasets from the GlyGen data repository (data.glygen.org). It is designed 
specifically for data science workflows, enabling the seamless conversion of 
remote CSV files into clean, filtered, and typed pandas DataFrames.

Features include:
- Automatic remote file discovery via glob patterns.
- Local caching of downloaded files.
- Memory-efficient processing using chunked reading.
- Dynamic data transformations, filtering, and static column injections.
- Intermediate DataFrame caching (via Feather format) for rapid reloading.
"""

import os
import sys
import time
import urllib.request
import re
import fnmatch
import shutil
import pandas as pd
from tqdm import tqdm

__version__ = "1.1.0"

class GlyGenDownloader(object):
    """
    A utility class to discover, download, cache, and load datasets from the GlyGen data repository 
    into pandas DataFrames seamlessly.
    """
    
    _base = "https://data.glygen.org/ln2data/releases/data/{VERSION}/reviewed/"
    _anchorre = re.compile(r'<a href="([^"]*)">([^<]*)</a>([^<]*)',re.MULTILINE)

    _glygentaxid = {
        "human": 9606,
        "mouse": 10090,
        "rat": 10116,
        "arabidopsis": 3702,
        "bovine": 9913,
        "chicken": 9031,
        "dicty": 44689,
        "fruitfly": 7227,
        "hamster": 10029,
        "hcv1a": 11108,
        "hcv1b": 11116,
        "pig": 9823,
        "sarscov1": 694009,
        "sarscov2": 2697049,
        "yeast": 4932,
        "zebrafish": 7955,
    }

    def __init__(self, verbose=True, **kwargs):
        """
        Initialize the GlyGenDownloader.

        Args:
            usecache (bool): If True, avoids re-downloading filesthat exist in the cache directory. Default: True.
            cachedir (str): Directory for download and DataFrame cache. Default: .glygen.
            dfcacheformat (str): Format for DataFrame cache. One of "fth" or "csv". Default: fth (Feather).
            clearcache (bool): If True, clear the cache upon initialization. Default: False.
            maxcacheage (float): Max. age of files in the cache, after which they must be re-downloaded or re-generated. In seconds. Default: 1 day.
            glygen_data_version (str): Release version of GlyGen data resource to retrieve datafiles from. Default: current.
            verbose (bool): If True, prints download progress and DataFrame summaries.
        """
        self._cache = kwargs.get("cachedir",".glygen")
        self._dfcache_format = kwargs.get("dfcacheformat","fth")
        self.verbose = verbose
        self.usecache = kwargs.get("usecache", True)
        self.maxcacheage = kwargs.get("maxcacheage",24*3600)
        self.glygen_data_version = kwargs.get("glygen_data_version","current")
        self.tqdm_min_size = kwargs.get("tqdm_min_size",10*1024**2)
        assert self._dfcache_format in ("fth","csv")
        if kwargs.get("clearcache",False):
            if os.path.isdir(self._cache):
                shutil.rmtree(self._cache)

    def _file_size(self, filename, units=None):
        """
        Internal method: Returns a human-readable file size for a given file.

        Args:
            filename (str): The path to the file.
            units (str, optional): Target unit (e.g., 'MB', 'GB'). If None, scales automatically.

        Returns:
            str: The formatted file size.
        """
        size_bytes = os.path.getsize(filename)
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if (size_bytes < 1024 and units is None) or (unit == units):
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.2f} PB"

    def listing(self, pattern, exclude=None, **kwargs):
        """
        Retrieves list of dictionaties with filenames and file sizes available on the GlyGen server that match a specific pattern.

        Args:
            pattern (str): A string formatting pattern or direct glob pattern to match (e.g., `"{species}_proteoform*"`).
            exclude (list of str, optional): Glob patterns to exclude from the results.
            **kwargs: Format arguments injected into the `pattern` string (e.g., `species="human"`).

        Returns:
            list: Alphabetically sorted list of dictionaries with filename and filebytes keys from the server.
        """
        glob_pattern = pattern.format(**kwargs)
        matched_files = []
        
        # Read the HTML directory listing from the GlyGen repository
        baseurl = self._base.format(VERSION=self.glygen_data_version)
        page = urllib.request.urlopen(baseurl).read().decode("utf-8")
        
        for m in self._anchorre.finditer(page):
            fn = m.group(1)
            rest = m.group(3).split()

            if len(rest) == 0:
                continue

            bytes = int(rest[-1])
            
            # Skip stat files automatically
            if fn.endswith('.stat.csv'):
                continue
            
            # Filter out explicitly excluded patterns
            if exclude is not None:
                if any((fn == exc_pattern or fnmatch.fnmatch(fn, exc_pattern)) for exc_pattern in exclude):
                    continue
            
            # Keep if the file matches our glob pattern
            if fnmatch.fnmatch(fn, glob_pattern):
                matched_files.append(dict(filename=fn,filebytes=bytes))
                
        return sorted(matched_files,key=lambda d: d.get('filename'))

    def filenames(self, pattern, exclude=None, **kwargs):
        """
        Retrieves list of filenames available on the GlyGen server that match a specific pattern.

        Args:
            pattern (str): A string formatting pattern or direct glob pattern to match (e.g., `"{species}_proteoform*"`).
            exclude (list of str, optional): Glob patterns to exclude from the results.
            **kwargs: Format arguments injected into the `pattern` string (e.g., `species="human"`).

        Returns:
            list: Alphabetically sorted list of matching filenames from the server.
        """
        return [ d['filename'] for d in self.listing(pattern, exclude=exclude, **kwargs) ]

    def download(self, filename, todir=None, filebytes=None):
        """
        Downloads a specific file from the GlyGen repository to a local cache directory.

        Args:
            filename (str): The name of the file to download.
            todir (str, optional): The target directory. Defaults to the `_cache` class attribute.
            filebytes (int, optional): The size in bytes expected for the file. If this value is provided 
                                       and the file in the cache does not match this value, it is 
                                       re-downloaded; if the downloaded file does not match this value, an 
                                       exception is raised; and a download progress bar is shown.
        Returns:
            str: The local filepath of the downloaded (or cached) file.
        """
        target_dir = todir if todir is not None else self._cache
        os.makedirs(target_dir, exist_ok=True)
        filepath = os.path.join(target_dir, filename)
        
        if not self.usecache or not os.path.exists(filepath) or \
            os.path.getmtime(filepath) < (time.time()-self.maxcacheage) or \
            (filebytes is not None and os.path.getsize(filepath) != filebytes):

            if os.path.exists(filepath):
                os.unlink(filepath)
            
            baseurl = self._base.format(VERSION=self.glygen_data_version)

            if filebytes is not None and filebytes >= self.tqdm_min_size and self.verbose:
                print(f"Download {filename}...", file=sys.stderr, flush=True)
                with tqdm(unit='B', unit_scale=True, unit_divisor=1024, 
                          miniters=1, desc="Download progress", ascii=True) as t:
                    def reporthook(block_num, block_size, total_size):
                        if total_size is not None:
                            t.total = total_size
                        # Update the progress bar based on the delta from the previous call
                        t.update(block_num * block_size - t.n)
                    urllib.request.urlretrieve(baseurl + filename, filepath, reporthook=reporthook)
                print(f"Download {filename}...", end="", file=sys.stderr, flush=True)
                print(f" done ({self._file_size(filepath)}).", file=sys.stderr, flush=True)
            else:
                if self.verbose:
                    print(f"Download {filename}...", end="", file=sys.stderr, flush=True)
                
                urllib.request.urlretrieve(baseurl + filename, filepath)

                if self.verbose:
                    print(f" done ({self._file_size(filepath)}).", file=sys.stderr, flush=True)
            
            if filebytes is not None and os.path.getsize(filepath) != filebytes:
                raise IOError(f"Downloaded file {filename} is truncated.")

            
        else:
            if self.verbose:
                print(f"Using cached {filename} ({self._file_size(filepath)}).", file=sys.stderr, flush=True)
                
        return filepath

    def _dataframe(self, *filenames, usecols=None, notna=None, asint=None,
                   setcolumn=None, transform=None, dropcols=None, dropdups=False,
                   addfilename=False, addspecies=False, addtaxid=False, filterrows=None):
        """
        Internal method: Reads and concatenates one or more CSV files into a single pandas DataFrame,
        applying cleaning, filtering, and transformation operations efficiently in chunks.
        """
        dfs = []
        
        # Handle cases where a list is passed as the first positional argument
        if len(filenames) == 1 and isinstance(filenames[0], (list, tuple)):
            filenames = filenames[0]
        
        if len(filenames) == 0:
            raise ValueError("No files provided to build data-frame.")

        listing = dict([(d['filename'],d['filebytes']) for d in self.listing("*")])

        for fn in filenames:
            if not os.path.exists(fn):
                fn = self.download(fn,filebytes=listing[fn])
                
            # Read in chunks to manage memory footprint efficiently
            for chunk_df in pd.read_csv(fn, usecols=usecols, chunksize=100000):
                # Drop rows where critical columns are missing
                if notna is not None:
                    for colname in notna:
                        chunk_df = chunk_df[~chunk_df[colname].isna()]
                
                # Cast specific columns to integers
                if asint is not None:
                    for colname in asint:
                        chunk_df[colname] = chunk_df[colname].astype(int)
                
                # Apply static column values
                if setcolumn is not None:
                    for k, v in setcolumn.items():
                        chunk_df[k] = v
                        
                # Extract filename and species contexts
                filename_only = os.path.split(fn)[1]
                species = filename_only.split("_", 1)[0]
                
                if addfilename:
                    chunk_df['filename'] = filename_only
                if addspecies and species in self._glygentaxid:
                    chunk_df['species'] = species
                if addtaxid and species in self._glygentaxid:
                    chunk_df['taxid'] = self._glygentaxid[species]
                    
                # Apply lambda transformations dynamically
                if transform is not None:
                    for k, v in transform.items():
                        chunk_df[k] = v(chunk_df)
                
                # Filter down to specific rows dynamically using lambdas
                if filterrows is not None:
                    for condition in filterrows:
                        chunk_df = chunk_df[condition(chunk_df)]
                        
                # Remove duplicate rows early to save memory
                if dropdups:
                    chunk_df = chunk_df.drop_duplicates()
                    
                dfs.append(chunk_df)
                
        # Safeguard against empty inputs yielding empty outputs
        if not dfs:
            return pd.DataFrame()
            
        # Compile all chunks together
        df = pd.concat(dfs, ignore_index=True)
        df.reset_index(inplace=True, drop=True)
        
        # Drop columns not needed post-filtering
        if dropcols is not None:
            df = df.drop(columns=dropcols)
            
        if dropdups:
            df = df.drop_duplicates()
            
        if self.verbose:
            print("Constructed DataFrame:\n", file=sys.stderr, flush=True)
            df.info(buf=sys.stderr)
            print(file=sys.stderr, flush=True)
            
        return df

    def dataframe(self, *filenames, **kwargs):
        """
        High-level API to build a cleaned, processed DataFrame from a list of files. 
        It provides a rich interface to apply lambdas and caching.

        Args:
            filenames (str or list): One or more filenames/paths to load and merge.
            name (str, optional): A unique alias for this DataFrame construction. If provided, 
                                  the processed DataFrame will be saved to the
                                  cache, speeding up future runs immensely.
            force (bool, optional): If True, ignores the cache and reconstructs the DataFrame.
            usecols (list, optional): Columns to extract from the source CSV.
            notna (list, optional): Columns that must not contain NaN values (rows dropped).
            asint (list, optional): Columns to cast as integers.
            setcolumn (dict, optional): Static columns to inject (e.g., `{"predicted": False}`).
            transform (dict, optional): Complex derivations passed as `{column_name: callable(df)}`. (e.g. `{"dstatus": lambda df: ~df["do_name"].isna()}`)
            filterrows (list of callables, optional): Condition functions to subset the data.
            dropcols (list, optional): Columns to discard at the very end of processing.
            dropdups (bool, optional): If True, applies `DataFrame.drop_duplicates()`.
            addfilename (bool, optional): If True, appends the source filename as a column.
            addspecies (bool, optional): If True, appends the inferred species as a column.
            addtaxid (bool, optional): If True, appends the species taxid as a column.

        Returns:
            pd.DataFrame: The finalized pandas DataFrame.
        """
        # Extract metadata keys gracefully without mutating the user's kwargs aggressively
        name = kwargs.pop("name", None)
        force = kwargs.pop("force", False)
        
        if name is None:
            return self._dataframe(*filenames, **kwargs)

        if len(filenames) == 1 and isinstance(filenames[0], (list, tuple)):
            filenames = filenames[0]
        maxmtime = 0; minmtime = 1e+20;
        for f in filenames:
            if os.path.exists(f):
                if os.path.getmtime(f) < maxmtime:
                    maxmtime = os.path.getmtime(f)
                if os.path.getmtime(f) > minmtime:
                    minmtime = os.path.getmtime(f)
        
        filename = os.path.join(self._cache, f"_dataframe_{name}.{self._dfcache_format}")

        if os.path.exists(filename) and self.usecache and not force and (min(minmtime,os.path.getmtime(filename)) > (time.time()-self.maxcacheage)) and (os.path.getmtime(filename) > maxmtime):
            if self.verbose:
                print(f"Reading cached DataFrame {name}...", end="", file=sys.stderr, flush=True)
            if self._dfcache_format == "fth":
                df = pd.read_feather(filename)
            elif self._dfcache_format == "csv":
                df = pd.read_csv(filename)
            if self.verbose:
                print(f"done. ({df.shape[0]} rows)\n", file=sys.stderr, flush=True)
            
            if self.verbose:
                df.info(buf=sys.stderr)
                print(file=sys.stderr, flush=True)
        else:
            df = self._dataframe(*filenames, **kwargs)
            if self.usecache:
                if self.verbose:
                    print(f"Writing DataFrame {name} to cache...", end="", file=sys.stderr, flush=True)
                if self._dfcache_format == "fth":
                    df.to_feather(filename)
                elif self._dfcache_format == "csv":
                    df.to_csv(filename,index=False)
                if self.verbose:
                    print(f" done. ({df.shape[0]} rows)\n", file=sys.stderr, flush=True)
            
        return df

if __name__ == "__main__":
    # ---------------------------------------------------------
    # Demonstration / Test block inspired by variants notebook
    # ---------------------------------------------------------
    
    print("Initializing GlyGenDownloader...")
    ggdl = GlyGenDownloader(glygen_data_version="v-2.10.1",maxagecache=10,verbose=True)
    SPECIES = "human"

    print(f"\n--- 1. Finding files for {SPECIES} ---")
    
    # UniProt files
    uniprot_template = "{species}_proteoform_glycosylation_sites_uniprotkb.csv"
    uniprotkb_site_files = ggdl.filenames(uniprot_template, species=SPECIES)
    
    # Predicted site files
    pred_template = "{species}_proteoform_glycosylation_sites_predicted_*.csv"
    pred_site_files = ggdl.filenames(pred_template, species=SPECIES)
    
    # Experimental site files (exclude the above two patterns)
    exp_template = "{species}_proteoform_glycosylation_sites_*.csv"
    exclude_patterns = [
        "*_proteoform_glycosylation_sites_uniprotkb.csv",
        "*_proteoform_glycosylation_sites_predicted_*.csv"
    ]
    exp_site_files = ggdl.filenames(exp_template, exclude=exclude_patterns, species=SPECIES)

    print(f"Found {len(uniprotkb_site_files)} UniProtKB files.")
    print(f"Found {len(pred_site_files)} Predicted files.")
    print(f"Found {len(exp_site_files)} Experimental files.")

    print("\n--- 2. Constructing Experimental Sites DataFrame ---")
    # Showcasing setting static columns and casting to int
    glyco_site_exp = ggdl.dataframe(
        exp_site_files,
        name="demo_glyco_site_exp",
        usecols=["uniprotkb_canonical_ac", "start_pos", "start_aa", "glycosylation_type"],
        notna=["uniprotkb_canonical_ac", "start_pos"],
        asint=["start_pos"],
        dropdups=True,
        setcolumn={"predicted": False}
    )

    print("\n--- 3. Constructing UniProtKB Sites DataFrame ---")
    # Showcasing lambda transformations and dropping columns dynamically
    glyco_site_uniprotkb = ggdl.dataframe(
        uniprotkb_site_files,
        name="demo_glyco_site_uniprotkb",
        usecols=["uniprotkb_canonical_ac", "start_pos", "start_aa", "glycosylation_type", "xref_key"],
        notna=["uniprotkb_canonical_ac", "start_pos"],
        asint=["start_pos"],
        dropdups=True,
        transform={
            "predicted": lambda df: ~df["xref_key"].isin(["protein_xref_pubmed", "protein_xref_doi"])
        },
        dropcols=["xref_key"]
    )

    print("\n--- 4. Final Summaries ---")
    print(f"Experimental Sites Shape: {glyco_site_exp.shape}")
    print(f"UniProtKB Sites Shape: {glyco_site_uniprotkb.shape}")