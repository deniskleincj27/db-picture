#!/usr/bin/env python3

import os.path, time, sys
from multiprocessing import Lock, Queue
import util

from PIL import Image
from PIL.ExifTags import TAGS, GPSTAGS

from geopy.geocoders import Nominatim
from geopy.geocoders import Bing
from geopy.extra.rate_limiter import RateLimiter

tiff_tag = [
  # TIFF Tag Reference, Baseline TIFF Tags, Image IFD
  (270,	'ImageDescription',	'A string that describes the subject of the image.'),
  (274,	'Orientation', 'The orientation of the image with respect to the rows and columns.'),
  (306,	'DateTime', 'Date and time of image creation.'),
  (315,	'Artist', 'Person who created the image.'),

  #  Exif IFD
  (36867, 'DateTimeOriginal', 'The date and time when the original image data was generated.'),
  (36868, 'DateTimeDigitized', 'The date and time when the image was stored as digital data.'),
  (37510, 'UserComment', 'Keywords or comments on the image; complements ImageDescription.'),
  (41728, 'FileSource', 'Indicates the image source.'),
  (41729, 'SceneType', 'Indicates the type of scene.')
]

# https://www.awaresystems.be/index.html
orientation_tag_list = [

  (1, 'Horizontal (normal)'),
  (2, 'Mirrored horizontal'),
  (3, 'Rotated 180'),
  (4, 'Mirrored vertical'),
  (5, 'Mirrored horizontal then rotated 90 CCW'),
  (6, 'Rotated 90 CW'),
  (7, 'Mirrored horizontal then rotated 90 CW'),
  (8, 'Rotated 90 CCW')

]

get_float = lambda x: float(x[0]) / float(x[1])

class Default(object):

  def __init__(self, *args):
    self.m_geolocator = None

  def get_geopy(self):
    return self.m_geolocator

  def get_picture_data(self, fname, geolocator):

    date = 'Unknown'
    location = 'Unknown'
    orientation = ''
    th_name = ''
    rz_name = ''

    return date, location, orientation, rz_name, th_name

class GeoPy(object):

  def __init__(self, *args):

    self.m_args = args
    api_key = 'AtbIr0Y1MYUHAdGI4spe2NLMCA0TOWUG1RRloo0NbZykVFjUdvEVqOYnSMxxuaJX'
    self.m_geolocator = Bing(api_key, timeout = 4)

  def get_geopy(self):
    return self.m_geolocator

  def resize_picture(self, fname, geolocator, angle = 0):

    _TAGS_r = dict(((v, k) for k, v in TAGS.items()))
    _GPSTAGS_r = dict(((v, k) for k, v in GPSTAGS.items()))

    th_name = None
    rz_name = None
    width = 0
    height = 0

    try:
      img = Image.open(fname)
      width, height = img.size
    except Exception as e:
      print('get_picture_data Exception e: ', e, ' fname: ', fname)
      width = -1
      height = -1
      return rz_name, th_name, width, height

    w = int(width)
    h = int(height)
    if w > h:
      w = 800
      h = 600
    else:
      w = 600
      h = 800
    rz_name = self.get_picture_thumbs(w, h, fname, '-resize.JPG', img, angle)

    w = 128
    h = 128
    th_name = self.get_picture_thumbs(w, h, fname, '-thumb.JPG', img)

    return rz_name, th_name, width, height

  def get_picture_data(self, fname, geolocator):

    _TAGS_r = dict(((v, k) for k, v in TAGS.items()))
    _GPSTAGS_r = dict(((v, k) for k, v in GPSTAGS.items()))

    date = 'Unknown'
    location = 'Unknown'
    orientation = ''
    th_name = ''
    rz_name = ''
    width = 0
    height = 0
    make = ''
    model = ''

    try:
      img = Image.open(fname)
      width, height = img.size
    except Exception as e:
      print('get_picture_data Exception e: ', e, ' fname: ', fname)
      width = -1
      height = -1
      return date, location, orientation, rz_name, th_name, width, height, make, model

    exifd = img._getexif()  # as dict
    try:
      keys = list(exifd.keys())
    except:
      return date, 'geo-nokeys', orientation, rz_name, th_name, width, height, make, model

    keys = [k for k in keys if k in TAGS]
    if (37510) in keys:
      keys.remove(_TAGS_r["UserComment"])
    if (37500) in keys:
      keys.remove(_TAGS_r["MakerNote"])

    try:
      img.close()
    except Exception as e:
      print('close Exception e: ', e, ' fname: ', fname)

    # symbolic name of keys
    for k in keys:
      val = exifd[k]
      res = type(val)
      if res == str:
        try:
          val = val.decode('utf-8')
        except:
          val = exifd[k]

      if (TAGS[k] == 'DateTime'):
        date = val
      if (TAGS[k] == 'Orientation'):
        orientation = self.get_orientation_tag(val)

      # 271  ==  Make
      if (TAGS[k] == 'Make'):
        make = val
      # 272  ==  Model
      if (TAGS[k] == 'Model'):
        model = val

    if (34853) not in keys:
      location = '34853 not in keys'
      return date, location, orientation, rz_name, th_name, width, height, make, model

    gpsinfo = exifd[_TAGS_r["GPSInfo"]]
    lat, lon = self.get_lat_lon(exifd)
    if lat == None or lon == None:
      location = lat
    else:
      coord = (lat, lon)
      try:
        location = geolocator.reverse(coord)
        if location == 'Unknown':
          print('location: ', fname, ' ', location, coord)
      except Exception as e:
        location = 'Service timed out'

    return date, location, orientation, rz_name, th_name, width, height, make, model

  def get_orientation_tag(self, val):

    otag = [otag for otag in orientation_tag_list if otag[0] == val]

    return otag[0]

  def get_tag_value(self, key):

    for tag_data in tiff_tag:
      _, strg, _ = tag_data
      if key.find(strg) != -1:
        return True

    return False

  def convert_to_degrees(self, value):
    d = get_float(value[0])
    m = get_float(value[1])
    s = get_float(value[2])
    return d + (m / 60.0) + (s / 3600.0)

  def get_lat_lon(self, info):

    try:
      gps_latitude = info[34853][2]
      gps_latitude_ref = info[34853][1]
      gps_longitude = info[34853][4]
      gps_longitude_ref = info[34853][3]
    except KeyError:
      return 'gps no latitude', None

    try:
      lat = self.convert_to_degrees(gps_latitude)
      if gps_latitude_ref != "N":
        lat *= -1

      lon = self.convert_to_degrees(gps_longitude)
      if gps_longitude_ref != "E":
        lon *= -1

    except KeyError:
      print('convert error: ', info[34853])
      return 'convert error', None

    return lat, lon


  def get_picture_thumbs(self, w, h, fname, f_ext, img, angle = 0):

    pos = fname.find('.')
    rname = fname[:pos]
    h_path = '/home/denis/Pictures/'

    pos = fname.find(h_path)
    assert pos != -1
    th_name = fname[pos + len(h_path) : ]
    pos = th_name.find('.')
    assert pos != -1
    th_name = th_name[ : pos]
    th_name = th_name + '-'+ str(w) + 'x' + str(h) + f_ext
    pos = f_ext.find('resize')

    if pos != -1:
      try:
        if angle != 0:
          img = img.rotate(angle)
        img_d = img.resize((w, h), Image.BICUBIC)

      except Exception as e:
        print('get_picture_thumbs 1 fname {0} e: {1} '.format(fname, e))
        return ''
    else:
      try:
        img.thumbnail((w, h), Image.BICUBIC)
        img_d = img
      except Exception as e:
        print('get_picture_thumbs 1 fname {0} e: {1} '.format(fname, e))
        return ''

    rname = rname + '-'+ str(w) + 'x' + str(h) + f_ext
    try:
      img_d.save(rname)
      img_d.close()
    except Exception as e:
      print('get_picture_thumbs 2 fname {} {} e: {} e: {}'.format(w, h, f_ext, e))
      return ''

    return th_name

def get_image_creation_date(file):

  # Convert seconds since epoch to readable timestamp
  modificationTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getctime(file)))
  print("Created: ", modificationTime )
  modificationTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(os.path.getmtime(file)))
  print("Last Modified Time : ", modificationTime )

