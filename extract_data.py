import oci
import sys
import time
from oci_services import OCIService

def execute_extract():
   # config = oci.config.from_file( "/.oci/config", "DEFAULT")

	# tenancy = Tenancy(config)
	# announcement = Announcement(config)

	# tenancy.print()
	# announcement.print()

   start_time = time.time()
   if len(sys.argv) > 1:
      authentication = sys.argv[1]
   else:
      authentication = "CONFIG"

   oci_service = OCIService( authentication )
   oci_service.extract_data()
   print("--- Execution time ---")
   print("--- %s seconds ---" % (time.time() - start_time))

execute_extract()
