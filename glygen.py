
import os, os.path
import urllib.request
import re, fnmatch

import pandas as pd


class GlyGenDownloader(object):
  _base = "https://data.glygen.org/ln2data/releases/data/current/reviewed/"
  _cache = ".glygen"
  
  def __init__(self,usecache=True,verbose=True):
    self.verbose = verbose
    self.usecache = usecache

  def file_size(self,filename,units=None):
    size_bytes = os.path.getsize(filename)
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if (size_bytes < 1024 and units is None) or (unit == units):
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"
  
  anchorre = re.compile(r'<a href="([^"]*)">([^<]*)</a>')

  def filenames(self,pattern,**kwargs):
    glob = pattern.format(**kwargs)

    fns = []
    page = urllib.request.urlopen(self._base).read().decode()
    for m in self.anchorre.finditer(page):
      fn = m.group(1)
      if fn.endswith('.stat.csv'):
        continue
      if fnmatch.fnmatch(fn,glob):
        fns.append(fn)
    return sorted(fns)

  def download(self,filename,todir=None):
    if todir is None:
      todir = self._cache
    else:
      todir = todir.rstrip(os.sep)
    todir += os.sep
    os.makedirs(todir,exist_ok=True)
    if not self.usecache or not os.path.exists(todir + filename):
      if self.verbose:
        print(f"Download {filename}...", end="")
      if os.path.exists(todir + filename):
        os.unlink(todir + filename)
      urllib.request.urlretrieve(self._base + filename, todir + filename)
      if self.verbose:
        print(f" done ({self.file_size(todir + filename)}).")
    else:
      if self.verbose:
        print(f"Using cached {filename} ({self.file_size(todir + filename)}).")
    return todir + filename
  
  def dataframe(self,*filenames,usecols=None,notna=None,asint=None,
                                setcolumn=None,transform=None,
                                dropcols=None,dropdups=False):
    dfs = []
    if isinstance(filenames[0],list) and len(filenames) == 1:
      filenames = filenames[0]
    for fn in filenames:
      if not os.path.exists(fn):
        fn = self.download(fn)
      for df in pd.read_csv(fn,usecols=usecols,chunksize=100000):
        if notna is not None:
          for colname in notna:
            df = df[~df[colname].isna()]
        if asint is not None:
          for colname in asint:
            df[colname] = df[colname].astype(int)
        if setcolumn is not None:
          for k,v in setcolumn.items():
            df[k] = v
        if transform is not None:
          for k,v in transform.items():
            df[k] = v(df)
        if dropdups:
          df = df.drop_duplicates()
        dfs.append(df)
    df = pd.concat(dfs,ignore_index=True)
    df.reset_index(inplace=True)
    if dropcols is not None:
      df = df.drop(columns=dropcols)
    if dropdups:
      df = df.drop_duplicates()
    if self.verbose:
      print("Constructed data-frame:")
      df.info()
      print()
    return df

if __name__ == "__main__":

  ggdl = GlyGenDownloader()

  site_files = ggdl.filenames("{species}_proteoform_glycosylation_sites_*.csv",species="human")

  print(site_files)

    
