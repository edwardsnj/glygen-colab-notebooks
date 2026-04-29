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
import urllib.request
import re
import fnmatch
import pandas as pd

__version__ = "1.0.3"

class GlyGenDownloader:
    """
    A utility to discover, download, cache, and load datasets from the GlyGen data repository 
    into pandas DataFrames seamlessly.
        
    Public Methods:
        filenames(pattern, exclude=None, **kwargs): Retrieves a list of available filenames matching a pattern.
        download(filename, todir=None): Downloads a specific file from the repository to local cache.
        dataframe(*filenames, **kwargs): High-level API to build a cleaned, processed DataFrame from a list of files.
    """
    
    _base = "https://data.glygen.org/ln2data/releases/data/current/reviewed/"
    _cache = ".glygen"
    _anchorre = re.compile(r'<a href="([^"]*)">([^<]*)</a>')
    
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

    def __init__(self, usecache=True, verbose=True):
        """
        Initialize the GlyGenDownloader.

        Args:
            usecache (bool): If True, avoids re-downloading files that exist in the cache directory.
            verbose (bool): If True, prints download progress and dataframe summaries.
        """
        self.verbose = verbose
        self.usecache = usecache

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

    def filenames(self, pattern, exclude=None, **kwargs):
        """
        Retrieves a list of filenames available on the GlyGen server that match a specific pattern.

        Args:
            pattern (str): A string formatting pattern or direct glob pattern to match (e.g., "{species}_proteoform*").
            exclude (list of str, optional): Glob patterns to exclude from the results.
            **kwargs: Format arguments injected into the `pattern` string (e.g., species="human").

        Returns:
            list: Alphabetically sorted list of matching filenames from the server.
        """
        glob_pattern = pattern.format(**kwargs)
        matched_files = []
        
        # Read the HTML directory listing from the GlyGen repository
        page = urllib.request.urlopen(self._base).read().decode("utf-8")
        
        for m in self._anchorre.finditer(page):
            fn = m.group(1)
            
            # Skip stat files automatically
            if fn.endswith('.stat.csv'):
                continue
            
            # Filter out explicitly excluded patterns
            if exclude is not None:
                if any((fn == exc_pattern or fnmatch.fnmatch(fn, exc_pattern)) for exc_pattern in exclude):
                    continue
            
            # Keep if the file matches our glob pattern
            if fnmatch.fnmatch(fn, glob_pattern):
                matched_files.append(fn)
                
        return sorted(matched_files)

    def download(self, filename, todir=None):
        """
        Downloads a specific file from the GlyGen repository to a local cache directory.

        Args:
            filename (str): The name of the file to download.
            todir (str, optional): The target directory. Defaults to the `_cache` class attribute.

        Returns:
            str: The local filepath of the downloaded (or cached) file.
        """
        target_dir = todir if todir is not None else self._cache
        os.makedirs(target_dir, exist_ok=True)
        filepath = os.path.join(target_dir, filename)
        
        if not self.usecache or not os.path.exists(filepath):
            if self.verbose:
                print(f"Download {filename}...", end="", file=sys.stderr, flush=True)
                
            if os.path.exists(filepath):
                os.unlink(filepath)
                
            urllib.request.urlretrieve(self._base + filename, filepath)
            
            if self.verbose:
                print(f" done ({self._file_size(filepath)}).", file=sys.stderr, flush=True)
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
            
        for fn in filenames:
            if not os.path.exists(fn):
                fn = self.download(fn)
                
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
            print("Constructed data-frame:\n", file=sys.stderr, flush=True)
            df.info(buf=sys.stderr)
            print(file=sys.stderr, flush=True)
            
        return df

    def dataframe(self, *filenames, **kwargs):
        """
        High-level API to build a cleaned, processed DataFrame from a list of files. 
        It provides a rich interface to apply lambdas and caching.

        Args:
            *filenames (str or list): One or more filenames/paths to load and merge.
            name (str, optional): A unique alias for this DataFrame construction. If provided, 
                                  the processed DataFrame will be saved to disk as a `.fth` 
                                  (Feather) cache file, speeding up future runs immensely.
            force (bool, optional): If True, ignores the `.fth` Feather cache and reconstructs the data.
            usecols (list, optional): Columns to extract from the source CSV.
            notna (list, optional): Columns that must not contain NaN values (rows dropped).
            asint (list, optional): Columns to cast as integers.
            setcolumn (dict, optional): Static columns to inject (e.g., {"predicted": False}).
            transform (dict, optional): Complex derivations passed as {column_name: callable(df)}.
                                        (e.g. {"dstatus": lambda df: ~df["do_name"].isna()}).
            filterrows (list of callables, optional): Condition functions to subset the data.
            dropcols (list, optional): Columns to discard at the very end of processing.
            dropdups (bool, optional): If True, applies DataFrame.drop_duplicates().
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
            
        filename = os.path.join(self._cache, f"_dataframe_{name}.fth")
        
        if os.path.exists(filename) and not force:
            print(f"Reading cached data-frame {name}...", end="", file=sys.stderr, flush=True)
            df = pd.read_feather(filename)
            print(f"done. ({df.shape[0]} rows)\n", file=sys.stderr, flush=True)
            
            if self.verbose:
                df.info(buf=sys.stderr)
                print(file=sys.stderr, flush=True)
        else:
            df = self._dataframe(*filenames, **kwargs)
            print(f"Writing data-frame {name} to cache...", end="", file=sys.stderr, flush=True)
            df.to_feather(filename)
            print(f"done. ({df.shape[0]} rows)\n", file=sys.stderr, flush=True)
            
        return df

if __name__ == "__main__":
    # ---------------------------------------------------------
    # Demonstration / Test block inspired by variants notebook
    # ---------------------------------------------------------
    
    print("Initializing GlyGenDownloader...")
    ggdl = GlyGenDownloader(verbose=True)
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
    print("\nDemonstration complete!")