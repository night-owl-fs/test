$ErrorActionPreference = 'Stop'
Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)

gdalbuildvrt "KTPA_Z13_3857.vrt" "13/*.png"
gdalwarp -t_srs EPSG:32617 -tr 0.5 0.5 -r bilinear -of COG "KTPA_Z13_3857.vrt" "KTPA_Z13_0.5m_EPSG32617.tif"

gdalbuildvrt "KTPA_Z14_3857.vrt" "14/*.png"
gdalwarp -t_srs EPSG:32617 -tr 0.5 0.5 -r bilinear -of COG "KTPA_Z14_3857.vrt" "KTPA_Z14_0.5m_EPSG32617.tif"

gdalbuildvrt "KTPA_Z15_3857.vrt" "15/*.png"
gdalwarp -t_srs EPSG:32617 -tr 0.5 0.5 -r bilinear -of COG "KTPA_Z15_3857.vrt" "KTPA_Z15_0.5m_EPSG32617.tif"

gdalbuildvrt "KTPA_Z16_3857.vrt" "16/*.png"
gdalwarp -t_srs EPSG:32617 -tr 0.5 0.5 -r bilinear -of COG "KTPA_Z16_3857.vrt" "KTPA_Z16_0.5m_EPSG32617.tif"

gdalbuildvrt "KTPA_Z17_3857.vrt" "17/*.png"
gdalwarp -t_srs EPSG:32617 -tr 0.5 0.5 -r bilinear -of COG "KTPA_Z17_3857.vrt" "KTPA_Z17_0.5m_EPSG32617.tif"

gdalbuildvrt "KTPA_Z18_3857.vrt" "18/*.png"
gdalwarp -t_srs EPSG:32617 -tr 0.5 0.5 -r bilinear -of COG "KTPA_Z18_3857.vrt" "KTPA_Z18_0.5m_EPSG32617.tif"

