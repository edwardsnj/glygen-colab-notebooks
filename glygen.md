
<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/main/glygen.py#L0"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square" /></a>

# <kbd>module</kbd> `glygen`
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


## Table of Contents
- [`GlyGenDownloader`](./glygen.md#class-glygendownloader): A utility class to discover, download, cache, and load datasets from the GlyGen data repository  into pandas DataFrames seamlessly.
	- [`__init__`](./glygen.md#constructor-__init__): Initialize the GlyGenDownloader.
	- [`dataframe`](./glygen.md#method-dataframe): High-level API to build a cleaned, processed DataFrame from a list of files.  It provides a rich interface to apply lambdas and caching.
	- [`download`](./glygen.md#method-download): Downloads a specific file from the GlyGen repository to a local cache directory.
	- [`filenames`](./glygen.md#method-filenames): Retrieves a list of filenames available on the GlyGen server that match a specific pattern.




---

<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/main/glygen.py#L27"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square" /></a>

## <kbd>class</kbd> `GlyGenDownloader`
A utility class to discover, download, cache, and load datasets from the GlyGen data repository 
into pandas DataFrames seamlessly.


<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/main/glygen.py#L56"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square" /></a>

### <kbd>constructor</kbd> `__init__`

```python
GlyGenDownloader(
    usecache=True,
    clearcache=False,
    maxcacheage=86400,
    verbose=True
)
```

Initialize the GlyGenDownloader.


**Args:**

- <b>`usecache`</b> (bool): If True, avoids re-downloading files that exist in the cache directory.
- <b>`clearcache`</b> (bool): If True, clear the cache upon initialization.
- <b>`maxcacheage`</b> (float): Max. age of files in the cache, after which they must be re-downloaded or re-generated. In seconds. Default: 1 day.
- <b>`verbose`</b> (bool): If True, prints download progress and dataframe summaries.





---

<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/main/glygen.py#L241"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square" /></a>

### <kbd>method</kbd> `dataframe`

```python
dataframe(*filenames, **kwargs)
```

High-level API to build a cleaned, processed DataFrame from a list of files. 
It provides a rich interface to apply lambdas and caching.


**Args:**

- <b>`filenames`</b> (str or list): One or more filenames/paths to load and merge.
- <b>`name`</b> (str, optional): A unique alias for this DataFrame construction. If provided, 
                      the processed DataFrame will be saved to disk as a `.fth` 
                      (Feather) cache file, speeding up future runs immensely.
- <b>`force`</b> (bool, optional): If True, ignores the `.fth` Feather cache and reconstructs the data.
- <b>`usecols`</b> (list, optional): Columns to extract from the source CSV.
- <b>`notna`</b> (list, optional): Columns that must not contain NaN values (rows dropped).
- <b>`asint`</b> (list, optional): Columns to cast as integers.
- <b>`setcolumn`</b> (dict, optional): Static columns to inject (e.g., `{"predicted": False}`).
- <b>`transform`</b> (dict, optional): Complex derivations passed as `{column_name: callable(df)}`. (e.g. `{"dstatus": lambda df: ~df["do_name"].isna()}`)
- <b>`filterrows`</b> (list of callables, optional): Condition functions to subset the data.
- <b>`dropcols`</b> (list, optional): Columns to discard at the very end of processing.
- <b>`dropdups`</b> (bool, optional): If True, applies `DataFrame.drop_duplicates()`.
- <b>`addfilename`</b> (bool, optional): If True, appends the source filename as a column.
- <b>`addspecies`</b> (bool, optional): If True, appends the inferred species as a column.
- <b>`addtaxid`</b> (bool, optional): If True, appends the species taxid as a column.


**Returns:**

- <b>`pd.DataFrame`</b>: The finalized pandas DataFrame.


---

<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/main/glygen.py#L126"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square" /></a>

### <kbd>method</kbd> `download`

```python
download(filename, todir=None)
```

Downloads a specific file from the GlyGen repository to a local cache directory.


**Args:**

- <b>`filename`</b> (str): The name of the file to download.
- <b>`todir`</b> (str, optional): The target directory. Defaults to the `_cache` class attribute.


**Returns:**

- <b>`str`</b>: The local filepath of the downloaded (or cached) file.


---

<a href="https://github.com/edwardsnj/glygen-colab-notebooks/blob/main/glygen.py#L90"><img align="right" style="float:right;" src="https://img.shields.io/badge/-source-cccccc?style=flat-square" /></a>

### <kbd>method</kbd> `filenames`

```python
filenames(pattern, exclude=None, **kwargs)
```

Retrieves a list of filenames available on the GlyGen server that match a specific pattern.


**Args:**

- <b>`pattern`</b> (str): A string formatting pattern or direct glob pattern to match (e.g., `"{species}_proteoform*"`).
- <b>`exclude`</b> (list of str, optional): Glob patterns to exclude from the results.
**kwargs: Format arguments injected into the `pattern` string (e.g., `species="human"`).


**Returns:**

- <b>`list`</b>: Alphabetically sorted list of matching filenames from the server.




