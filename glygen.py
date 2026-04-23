
import pandas as pd

class GlyGenDownloader(object):
  _base = GLYGEN_DATA_REVIEWED = "https://data.glygen.org/ln2data/releases/data/current/reviewed/"
  _cache = ".glygen/"
  
  def __init__(self,usecache=True,verbose=True):
    self.verbose = verbose
    self.usecache = usecache

  def file_size(self,filename,units=None):
    size_bytes = os.path.getsize(path)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if (size_bytes < 1024 and units is None) or (unit == units):
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"
  
  def download(self,filename):
    os.makedirs(self._cache)
    if not self.usecache or not os.path.exists(self._cache + filename)
      if self.verbose:
        print(f"Download {filename}...", end="")
      if os.path.exists(self._cache + filename):
        os.unlink(self._cache + filename)
      urllib.urlretrieve(self._base + filename, self._cache + filename)
      if self.verbose:
        print(f" done ({self.file_size(self._cache + filename}).")
    else:
      if self.verbose:
        print(f"Using cached {filename} ({self.file_size(self._cache + filename}).")
    return self._cache + filename
  
  def dataframe(self,*filenames,usecols=None,notna=None,asint=None,dropdups=False):
    dfs = []
    for fn in filenames:
      fn = self.download(fn)
      for df in pd.read_csv(fn,usecols=usecols,chunksize=100000):
        if notna is not None:
          for colname in notna:
            df = df[~df[colname].isna()]
        if asint is not None:
          for colname in asint:
            df[colname] = df[colname].asint()
        if dropdups:
          df = df.drop_duplicates()
        dfs.append(df)
    df = pd.concat(dfs,ignore_index=True)
    if dropdups:
      df = df.drop_duplicates()
    return df

    
    
