<a id="glygen"></a>

# glygen

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

<a id="glygen.GlyGenDownloader"></a>

## GlyGenDownloader Objects

```python
class GlyGenDownloader(object)
```

A utility to discover, download, cache, and load datasets from the GlyGen data repository
into pandas DataFrames seamlessly.

Public Methods:
filenames(pattern, exclude=None, **kwargs): Retrieves a list of available filenames matching a pattern.
download(filename, todir=None): Downloads a specific file from the repository to local cache.
dataframe(*filenames, **kwargs): High-level API to build a cleaned, processed DataFrame from a list of files.

<a id="glygen.GlyGenDownloader.__init__"></a>

#### \_\_init\_\_

```python
def __init__(usecache=True, verbose=True)
```

Initialize the GlyGenDownloader.

**Arguments**:

- `usecache` _bool_ - If True, avoids re-downloading files that exist in the cache directory.
- `verbose` _bool_ - If True, prints download progress and dataframe summaries.

<a id="glygen.GlyGenDownloader.filenames"></a>

#### filenames

```python
def filenames(pattern, exclude=None, **kwargs)
```

Retrieves a list of filenames available on the GlyGen server that match a specific pattern.

**Arguments**:

- `pattern` _str_ - A string formatting pattern or direct glob pattern to match (e.g., "{species}_proteoform*").
- `exclude` _list of str, optional_ - Glob patterns to exclude from the results.
- `**kwargs` - Format arguments injected into the `pattern` string (e.g., species="human").
  

**Returns**:

- `list` - Alphabetically sorted list of matching filenames from the server.

<a id="glygen.GlyGenDownloader.download"></a>

#### download

```python
def download(filename, todir=None)
```

Downloads a specific file from the GlyGen repository to a local cache directory.

**Arguments**:

- `filename` _str_ - The name of the file to download.
- `todir` _str, optional_ - The target directory. Defaults to the `_cache` class attribute.
  

**Returns**:

- `str` - The local filepath of the downloaded (or cached) file.

<a id="glygen.GlyGenDownloader.dataframe"></a>

#### dataframe

```python
def dataframe(*filenames, **kwargs)
```

High-level API to build a cleaned, processed DataFrame from a list of files.
It provides a rich interface to apply lambdas and caching.

**Arguments**:

- `*filenames` _str or list_ - One or more filenames/paths to load and merge.
- `name` _str, optional_ - A unique alias for this DataFrame construction. If provided,
  the processed DataFrame will be saved to disk as a `.fth`
  (Feather) cache file, speeding up future runs immensely.
- `force` _bool, optional_ - If True, ignores the `.fth` Feather cache and reconstructs the data.
- `usecols` _list, optional_ - Columns to extract from the source CSV.
- `notna` _list, optional_ - Columns that must not contain NaN values (rows dropped).
- `asint` _list, optional_ - Columns to cast as integers.
- `setcolumn` _dict, optional_ - Static columns to inject (e.g., {"predicted": False}).
- `transform` _dict, optional_ - Complex derivations passed as {column_name: callable(df)}.
  (e.g. {"dstatus": lambda df: ~df["do_name"].isna()}).
- `filterrows` _list of callables, optional_ - Condition functions to subset the data.
- `dropcols` _list, optional_ - Columns to discard at the very end of processing.
- `dropdups` _bool, optional_ - If True, applies DataFrame.drop_duplicates().
- `addfilename` _bool, optional_ - If True, appends the source filename as a column.
- `addspecies` _bool, optional_ - If True, appends the inferred species as a column.
- `addtaxid` _bool, optional_ - If True, appends the species taxid as a column.
  

**Returns**:

- `pd.DataFrame` - The finalized pandas DataFrame.

