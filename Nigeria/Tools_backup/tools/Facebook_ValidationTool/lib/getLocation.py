# import module
import certifi
import ssl
import geopy.geocoders
from geopy.geocoders import Nominatim

# initialize Nominatim API
ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx

geolocator = Nominatim(user_agent="myGeocoder")
def main(args):
# Latitude & Longitude input
    Latitude = str(args[0])
    Longitude = str(args[1])

    location = geolocator.reverse(Latitude+","+Longitude)

    address = location.raw['address']

# traverse the data
    country_code = address.get('country_code', '')
    print(country_code.upper())
    #return country_code.upper()

if __name__ == '__main__':
    import sys
    main(sys.argv[1:])
