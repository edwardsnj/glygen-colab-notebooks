
import os, os.path
import urllib
import re

import pandas as pd


class GlyGenDownloader(object):
  _base = "https://data.glygen.org/ln2data/releases/data/current/reviewed/"
  _cache = ".glygen"
  
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
  
  def filenames(self,pattern,**kwargs):
    fns = []
    page = urllib.request.urlopen(self._base).read()
    glob = pattern.format(kwargs)
    for m in re.iterfind(r'<a href="([^"]*)">([^<]*)</a>'):
      fn = m.group(2)
      if fnmatch.fnmatch(fn,glob):
        fns.append(fns)
    return fns

  def download(self,filename,todir=None):
    if todir is None:
      todir = self._cache
    else:
      todir = todir.rstrip(os.sep)
    todir += os.sep
    os.makedirs(todir)
    if not self.usecache or not os.path.exists(todir + filename):
      if self.verbose:
        print(f"Download {filename}...", end="")
      if os.path.exists(todir + filename):
        os.unlink(todir + filename)
      urllib.urlretrieve(self._base + filename, todir + filename)
      if self.verbose:
        print(f" done ({self.file_size(todir + filename)}).")
    else:
      if self.verbose:
        print(f"Using cached {filename} ({self.file_size(todir + filename)}).")
    return todir + filename
  
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

    
    
