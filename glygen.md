
<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/devel/glygen.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

# <kbd>module</kbd> `glygen.py`
GlyGen Data Utilities 

This module provides utilities to discover, download, cache, and process  datasets from the GlyGen data repository (data.glygen.org). It is designed  specifically for data science workflows, enabling the seamless conversion of  remote CSV files into clean, filtered, and typed pandas DataFrames. 

Features include: 
- Automatic remote file discovery via glob patterns. 
- Local caching of downloaded files. 
- Memory-efficient processing using chunked reading. 
- Dynamic data transformations, filtering, and static column injections. 
- Intermediate DataFrame caching (via Feather format) for rapid reloading. 



---

## <kbd>class</kbd> `GlyGenDownloader`
A utility to discover, download, cache, and load datasets from the GlyGen data repository  into pandas DataFrames seamlessly.  

Public Methods:  filenames(pattern, exclude=None, **kwargs): Retrieves a list of available filenames matching a pattern.  download(filename, todir=None): Downloads a specific file from the repository to local cache.  dataframe(*filenames, **kwargs): High-level API to build a cleaned, processed DataFrame from a list of files. 

<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/devel/glygen.py#L60"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>function</kbd> `__init__`

```python
__init__(usecache=True, verbose=True)
```

Initialize the GlyGenDownloader. 



**Args:**
 
 - <b>`usecache`</b> (bool):  If True, avoids re-downloading files that exist in the cache directory. 
 - <b>`verbose`</b> (bool):  If True, prints download progress and dataframe summaries. 




---

<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/devel/glygen.py#L240"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>function</kbd> `dataframe`

```python
dataframe(*filenames, **kwargs)
```

High-level API to build a cleaned, processed DataFrame from a list of files.  It provides a rich interface to apply lambdas and caching. 



**Args:**
 
 - <b>`*filenames (str or list)`</b>:  One or more filenames/paths to load and merge. 
 - <b>`name`</b> (str, optional):  A unique alias for this DataFrame construction. If provided,   the processed DataFrame will be saved to disk as a `.fth`   (Feather) cache file, speeding up future runs immensely. 
 - <b>`force`</b> (bool, optional):  If True, ignores the `.fth` Feather cache and reconstructs the data. 
 - <b>`usecols`</b> (list, optional):  Columns to extract from the source CSV. 
 - <b>`notna`</b> (list, optional):  Columns that must not contain NaN values (rows dropped). 
 - <b>`asint`</b> (list, optional):  Columns to cast as integers. 
 - <b>`setcolumn`</b> (dict, optional):  Static columns to inject (e.g., {"predicted": False}). 
 - <b>`transform`</b> (dict, optional):  Complex derivations passed as {column_name: callable(df)}. (e.g. {"dstatus": lambda df: ~df["do_name"].isna()}) 
 - <b>`filterrows`</b> (list of callables, optional):  Condition functions to subset the data. 
 - <b>`dropcols`</b> (list, optional):  Columns to discard at the very end of processing. 
 - <b>`dropdups`</b> (bool, optional):  If True, applies DataFrame.drop_duplicates(). 
 - <b>`addfilename`</b> (bool, optional):  If True, appends the source filename as a column. 
 - <b>`addspecies`</b> (bool, optional):  If True, appends the inferred species as a column. 
 - <b>`addtaxid`</b> (bool, optional):  If True, appends the species taxid as a column. 



**Returns:**
 
 - <b>`pd.DataFrame`</b>:  The finalized pandas DataFrame. 

---

<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/devel/glygen.py#L125"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>function</kbd> `download`

```python
download(filename, todir=None)
```

Downloads a specific file from the GlyGen repository to a local cache directory. 



**Args:**
 
 - <b>`filename`</b> (str):  The name of the file to download. 
 - <b>`todir`</b> (str, optional):  The target directory. Defaults to the `_cache` class attribute. 



**Returns:**
 
 - <b>`str`</b>:  The local filepath of the downloaded (or cached) file. 

---

<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/devel/glygen.py#L89"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square"></a>

### <kbd>function</kbd> `filenames`

```python
filenames(pattern, exclude=None, **kwargs)
```

Retrieves a list of filenames available on the GlyGen server that match a specific pattern. 



**Args:**
 
 - <b>`pattern`</b> (str):  A string formatting pattern or direct glob pattern to match (e.g., "{species}_proteoform*"). 
 - <b>`exclude`</b> (list of str, optional):  Glob patterns to exclude from the results. 
 - <b>`**kwargs`</b>:  Format arguments injected into the `pattern` string (e.g., species="human"). 



**Returns:**
 
 - <b>`list`</b>:  Alphabetically sorted list of matching filenames from the server. 



