import oci
import time
import datetime
import requests
import logging
import socket
import sys
import os
from logging.handlers import SysLogHandler
from threading import Thread
from traceback import format_exception

### Global Variables ###
########################
config = None
signer = None
report_no = None
par_url = None
########################

### Check APP Name ###
######################
try:
  app_name = sys.argv[2]
except Exception:
   app_name = 'NONE'
######################

### LOGGER ###
##############
# Pass the two as env variables when runing the docker container
logging_address = os.environ['LOGGING_ADDRESS']
logging_port = os.environ['LOGGING_PORT']

# log to CSA VM on TCP
syslog = SysLogHandler(address=(logging_address, int(logging_port)))
format = f'%(asctime)s {app_name}: %(levelname)s : %(lineno)d : %(message)s'
formatter = logging.Formatter(format, datefmt='%b %d %H:%M:%S')
syslog.setFormatter(formatter)

logger = logging.getLogger()
logger.addHandler(syslog)
logger.setLevel(logging.INFO)
##############

### Uncaught Exception Handler ###
##################################
def my_handler(type, value, tb):
   #logger.exception('Uncaught exception: {0}'.format(str(value)))
   logger.error("*********************************************************************")
   logger.error("* An uncaught error was raised !!!                                  *")
   logger.error("*********************************************************************")
   logger.error("")
   logger.exception(format_exception(type, value, tb))

# Install exception handler
sys.excepthook = my_handler
##################################

### Create Custom Retry Strategy ###
####################################
retry_strategy_via_constructor = oci.retry.RetryStrategyBuilder(
   # Make up to 10 service calls
   max_attempts_check=True,
   max_attempts=10,

   # Don't exceed a total of 600 seconds for all service calls
   total_elapsed_time_check=True,
   total_elapsed_time_seconds=600,

   # Wait 60 seconds between attempts
   retry_max_wait_between_calls_seconds=60,

   # Use 2 seconds as the base number for doing sleep time calculations
   retry_base_sleep_time_seconds=2,

   # Retry on certain service errors:
   #
   #   - 5xx code received for the request
   #   - Any 429 (this is signified by the empty array in the retry config)
   #   - 400s where the code is QuotaExceeded or LimitExceeded
   service_error_check=True,
   service_error_retry_on_any_5xx=True,
   service_error_retry_config={
      400: ['QuotaExceeded', 'LimitExceeded'],
      429: []
   },

   # Use exponential backoff and retry with full jitter, but on throttles use
   # exponential backoff and retry with equal jitter
   backoff_type=oci.retry.BACKOFF_FULL_JITTER_EQUAL_ON_THROTTLE_VALUE
).get_retry_strategy()
####################################

logger.info("### START ###")


class OCIService(object):
   def __init__(self, authentication):
      global report_no
      global par_url
      
      # source the config file
      self.config = oci.config.from_file( "/.oci/config", "DEFAULT")
      par_url = self.config[ 'par' ]   

      # if intance pricipals - generate signer from token or config
      if( authentication == 'CONFIG' ):
         logger.info("Generate Auth signer from config file.")
         self.generate_signer_from_config()
      else:
         logger.info("Generate Auth signer from instance principal.")
         self.generate_signer_from_instance_principals()
      
      # time var for report number
      timetup = time.gmtime()
      report_no = time.strftime('%Y-%m-%dT%H:%M:%SZ', timetup).replace( ':', '-')

   def extract_data(self):
      logger.info("Data Extract & Data Upload processes initated. Please wait...")
      
      logger.debug("Initiate Data Extract objects...")
      tenancy = Tenancy(self.config, self.signer)
      announcement = Announcement(self.config, self.signer)
      limit = Limit( self.config, tenancy, self.signer )
      compute = Compute( self.config, tenancy, self.signer)
      block_storage = BlockStorage(self.config, tenancy, self.signer)    
      db_system = DBSystem( self.config, tenancy, self.signer )
      monitoring = Monitoring( self.config, tenancy, self.signer )  
      images = Images( self.config, tenancy, self.signer)
      logger.info("Data extraction finished.")
      
      # Create threads for "create_csv" methods 
      thread1 = Thread(target = tenancy.create_csv)
      thread2 = Thread(target = announcement.create_csv(self.config))
      thread3 = Thread(target = limit.create_csv(self.config))
      thread4 = Thread(target = compute.create_csv)
      thread5 = Thread(target = block_storage.create_csv)
      thread6 = Thread(target = db_system.create_csv(self.config))
      thread7 = Thread(target = monitoring.create_csv(self.config))
      thread8 = Thread(target = images.create_csv)
      
      logger.debug("Starting to write data to Object storage...")
      thread1.start()
      thread2.start()
      thread3.start()
      thread4.start()
      thread5.start()
      thread6.start()
      thread7.start()
      thread8.start()
      thread1.join()
      thread2.join()
      thread3.join()
      thread4.join()
      thread5.join()
      thread6.join()
      thread7.join()
      thread8.join()
      
      logger.info("Data upload to Object Storage finished.")
      logger.info("### END ###")

   ### Generate Signer from config ###
   ###################################
   def generate_signer_from_config(self):
      # create signer from config for authentication
      self.signer = oci.signer.Signer(
         tenancy=self.config["tenancy"],
         user=self.config["user"],
         fingerprint=self.config["fingerprint"],
         private_key_file_location=self.config.get("key_file"),
         #pass_phrase=oci.config.get_config_value_or_default(self.config, "pass_phrase"),
         #private_key_content=self.config.get("key_content")
      )

   ### Generate Signer from instance_principals ###
   ################################################
   def generate_signer_from_instance_principals(self):
      try:
         # get signer from instance principals token
         self.signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
      except Exception:
         logger.error("*********************************************************************")
         logger.error("* Error obtaining instance principals certificate.                  *")
         logger.error("* Aboting.                                                          *")
         logger.error("*********************************************************************")
         logger.error("")
         raise SystemExit

      # generate config info from signer
      self.config = {'region': self.signer.region, 'tenancy': self.signer.tenancy_id}

class Tenancy(object):
   logger.info("Initiate Tennancy object...")
   
   tenancy_id = None
   name = None
   description = None
   home_region = None

   compartments = []
   regions = None
   availability_domains = []
   limit_summary = []

   def __init__(self, config, signer):
      self.tenancy_id = config["tenancy"]

      # get the identity client & tenancy objects
      identity_client = oci.identity.IdentityClient(config, signer=signer )
      tenancy = identity_client.get_tenancy( self.tenancy_id, retry_strategy=retry_strategy_via_constructor ).data

      self.name = tenancy.name
      self.description = tenancy.description
      self.home_region = tenancy.home_region_key

      # get list of regions
      self.regions = identity_client.list_region_subscriptions( self.tenancy_id, retry_strategy=retry_strategy_via_constructor ).data
      logger.debug(" --- List of regions is --- ")
      logger.debug(self.regions)

      # create compartments list
      self.compartments.append( oci.identity.models.Compartment(compartment_id=tenancy.id, name=f'{tenancy.name} (root)', description=tenancy.description, id=tenancy.id) )
      self.compartments += identity_client.list_compartments( self.tenancy_id, compartment_id_in_subtree=True, access_level="ACCESSIBLE", retry_strategy=retry_strategy_via_constructor ).data
      logger.debug(" --- List of compartments is --- ")
      logger.debug(self.compartments)
      
      # loop over each region
      for region in self.regions:
         signer.region = region.region_name
         identity_client = oci.identity.IdentityClient(config, signer=signer)
         
         # add ADs for each region
         self.availability_domains += identity_client.list_availability_domains(self.tenancy_id, retry_strategy=retry_strategy_via_constructor).data

      logger.debug(" --- List of ADs is --- ")
      logger.debug(self.availability_domains)
      
      logger.info("Tenancy - DONE.")
      
   ### return the list of ACTIVE compartments ###
   ##############################################
   def get_compartments(self):
      return [c for c in self.compartments if ( c.lifecycle_state == 'ACTIVE' and c.name != 'ManagedCompartmentForPaaS' and c.name != 'OCI_Scripts' )]

   ### return the list of ADs for a specific region ###
   ####################################################
   def get_availability_domains( self, region_name):
      #return [e for e in availability.domains if e.region_name == region_name]
      data = []
      for ad in self.availability_domains:
         s = ad.name.split( '-')
         name = f'{s[0][5:].lower()}-{s[1].lower()}-{s[2].lower()}'
         if name == region_name.lower():
            data.append( ad )

      return data

   ### upload tennancy data to object storage ###
   ##############################################
   def create_csv(self):
      # Report
      data = 'tenancy_id, report_no'
      data += '\n'
      data += f'{self.tenancy_id}, {report_no}'

      write_file( data, 'report' )

      # Region
      data = 'tenancy_id, region_key, region_name, is_home_region, report_no'
      
      for region in self.regions:
         data += '\n'
         data += f'{self.tenancy_id}, {region.region_key}, {region.region_name}, {region.is_home_region}, {report_no}'
      
      write_file( data, 'region' )

      # Compartment
      data = 'compartment_id, name, description, tenancy_id, report_no'      

      for compartment in self.compartments:
         data += '\n'
         data += f'{compartment.id}, {compartment.name}, {compartment.description}, {compartment.compartment_id}, {report_no}'

      write_file( data, 'compartment' )

      # Availability Domains
      data = 'ad_id, ad_name, tenancy_id, region_name, report_no'
      for ad in self.availability_domains:
         s = ad.name.split( '-')
         data += '\n'
         data += f'{ad.id}, {ad.name}, {ad.compartment_id}, {s[0][5:].lower()}-{s[1].lower()}-{s[2].lower()}, {report_no}'

      write_file( data, 'availability_domain' )

class Announcement(object):
   logger.info("Initiate Announcement object...")
   
   annoucements = []

   def __init__(self, config, signer):      
      # get list of announcements
      announcement_service = oci.announcements_service.AnnouncementClient(config, signer=signer )
      self.announcements = announcement_service.list_announcements( config[ "tenancy" ], lifecycle_state=oci.announcements_service.models.AnnouncementSummary.LIFECYCLE_STATE_ACTIVE, sort_by="timeCreated", retry_strategy=retry_strategy_via_constructor ).data

      logger.debug(" --- List of Announcements is --- ")
      logger.debug(self.announcements)
      
      logger.info("Announcement - DONE.")
      
   ### upload Announcement data to object storage ###
   ##################################################
   def create_csv(self, config):
      self.tenancy_id = config["tenancy"]
      
      data = 'affected_regions, announcement_type, announcement_id, reference_ticket_number, services, summary, time_updated, type, tenancy_id, report_no'

      for announcement in self.announcements.items:
         affected_regions = str(announcement.affected_regions).strip( '[]' ).replace( ',', '/' ).replace( "'",'' )
         services = str(announcement.services).strip( '[]' ).replace( ',', '/' ).replace( "'",'' )
         data += '\n'
         data += f'{affected_regions}, {announcement.announcement_type}, {announcement.id}, {announcement.reference_ticket_number}, {services}, {announcement.summary}, {announcement.time_updated}, {announcement.type}, {self.tenancy_id}, {report_no}'
         
      write_file( data, 'announcement' )

class Limit(object):
   logger.info("Initiate Limit object...")
   
   limit_summary = []

   def __init__(self, config, tenancy, signer):
      tenancy_id = config[ "tenancy" ]
      jobs = []

      # loop over all regions
      for region in tenancy.regions:
         signer.region = region.region_name
         
         limits_client = oci.limits.LimitsClient(config, signer=signer)
         services = limits_client.list_services( tenancy_id, sort_by="name", retry_strategy=retry_strategy_via_constructor).data      

         if services:
            # oci.limits.models.ServiceSummary
            for service in services:            
               # get the limits per service
               limits = limits_client.list_limit_values(tenancy_id, service_name=service.name, sort_by="name", retry_strategy=retry_strategy_via_constructor).data
               
               # initiate thread for service
               thread = Thread(target = self.get_info, args=(service, limits_client, limits, tenancy_id, tenancy, signer.region))
               jobs.append(thread)
         
      # start threads   
      for job in jobs:
         job.start()
         
      # join threads so we don't quit until all threads have finished
      for job in jobs:
         job.join()
         
      logger.debug(" --- List of Limits is --- ")
      logger.debug(self.limit_summary)
      
      logger.info("Limit - DONE.")
      
   ### thread function - get all limits ###
   ########################################
   def get_info(self, service, limits_client, limits, tenancy_id, tenancy, region):
      for limit in limits:
         val = {
                  'service_name': str(service.name),
                  'service_description': str(service.description),
                  'limit_name': str(limit.name),
                  'availability_domain': ("" if limit.availability_domain is None else str(limit.availability_domain)),
                  'scope_type': str(limit.scope_type),
                  'value': str(limit.value),
                  'used': "",
                  'available': "",
                  'region_name': str(region)
         }

         # if not limit, continue, don't calculate limit = 0
         if limit.value == 0:
            continue

         # get usage per limit if available
         usage = []
         
         if limit.scope_type == "AD":
            usage = limits_client.get_resource_availability(service.name, limit.name, tenancy_id, availability_domain=limit.availability_domain, retry_strategy=retry_strategy_via_constructor).data
         else:
            usage = limits_client.get_resource_availability(service.name, limit.name, tenancy_id, retry_strategy=retry_strategy_via_constructor).data

         # oci.limits.models.ResourceAvailability
         if usage.used:
            val['used'] = str(usage.used)
            
         if usage.available:
            val['available'] = str(usage.available)

         self.limit_summary.append(val)
         
   ### upload Limit data to object storage ###
   ###########################################
   def create_csv(self, config):
      self.tenancy_id = config["tenancy"]
      
      data = 'region_name, service_name, service_description, limit_name, availability_domain, scope_type, value, used, available, tenancy_id, report_no'
      for limit in self.limit_summary:
         data += '\n'
         data += f"{limit['region_name']}, {limit['service_name']}, {limit['service_description']}, {limit['limit_name']}, {limit['availability_domain']}, {limit['scope_type']}, {limit['value']}, {limit[ 'used' ]}, {limit[ 'available' ]}, {self.tenancy_id}, {report_no}"

      write_file( data, 'limit' )


class Images(object):
   logger.info("Initiate Images object...")
   
   images = []
   
   def __init__(self, config, tenancy, signer):
      self.tenancy_id = config[ 'tenancy']
      jobs = []
      
      # loop over all regions
      for region in tenancy.regions:
         signer.region = region.region_name
         compute_client = oci.core.ComputeClient(config, signer=signer)
         
         # loop over all compartments in each region
         for c in tenancy.get_compartments():
            # initiate a thread for each compartment
            thread = Thread(target = self.get_info, args=(c, compute_client, tenancy, region))
            jobs.append(thread)
      
      # start threads
      for job in jobs:
         job.start()
         
      # join threads so we don't quit until all threads have finished
      for job in jobs:
         job.join()
                  
      logger.debug(" --- List of Images is --- ")
      logger.debug(self.images)
   
   ### thread function - get all info about images ###
   ######################################################
   def get_info(self, c, compute_client, tenancy, region):
      # get all images
      self.images += compute_client.list_images(c.id, retry_strategy=retry_strategy_via_constructor).data
   
   ### upload images data to object storage ###
   #############################################          
   def create_csv(self):
      # images
      data = 'agent_features, base_image_id, compartment_id, display_name, id, launch_mode, boot_volume_type, firmware, network_type, operating_system, operating_system_version, size_in_mbs, time_created, report_no'

      for image in self.images:
         data += '\n'
         data += f'{image.agent_features}, {image.base_image_id}, {image.compartment_id}, {image.display_name}, {image.id}, {image.launch_mode}, {image.launch_options.boot_volume_type}, {image.launch_options.firmware}, {image.launch_options.network_type}, {image.operating_system}, {image.operating_system_version}, {image.size_in_mbs}, {image.time_created}, {report_no}'

      write_file( data, 'image' )
 
 
class Compute(object):
   logger.info("Initiate Compute object...")
   
   dedicated_hosts = []
   instances = []
   bv_attachments = []
   vol_attachments = []
   tenancy_id = None

   def __init__(self, config, tenancy, signer):
      self.tenancy_id = config[ 'tenancy']
      jobs = []
      
      # loop over all regions
      for region in tenancy.regions:
         signer.region = region.region_name
         compute_client = oci.core.ComputeClient(config, signer=signer)
         
         # loop over all compartments in each region
         for c in tenancy.get_compartments():
            # initiate a thread for each compartment
            thread = Thread(target = self.get_info, args=(c, compute_client, tenancy, region))
            jobs.append(thread)
      
      # start threads
      for job in jobs:
         job.start()
         
      # join threads so we don't quit until all threads have finished
      for job in jobs:
         job.join()
                  
      logger.debug(" --- List of Dedicated Hosts is --- ")
      logger.debug(self.dedicated_hosts)
      logger.debug(" --- List of Instances is --- ")
      logger.debug(self.instances)
      logger.debug(" --- List of Volume Attachments is --- ")
      logger.debug(self.vol_attachments)
      logger.debug(" --- List of Boot Volume Attachments is --- ")
      logger.debug(self.bv_attachments)
               
      logger.info("Compute - DONE.")
      
   ### thread function - get all info about instances ###
   ######################################################
   def get_info(self, c, compute_client, tenancy, region):
      # get all dedicated hosts
      self.dedicated_hosts += compute_client.list_dedicated_vm_hosts(c.id, retry_strategy=retry_strategy_via_constructor).data
      # get all instances
      self.instances += compute_client.list_instances(c.id, retry_strategy=retry_strategy_via_constructor).data
      # get all volume attachments
      self.vol_attachments += compute_client.list_volume_attachments(c.id, retry_strategy=retry_strategy_via_constructor).data
            
      ads = tenancy.get_availability_domains(region.region_name)
      
      for ad in ads:
         # get all boot volume attachments
         self.bv_attachments += compute_client.list_boot_volume_attachments( ad.name, c.id, retry_strategy=retry_strategy_via_constructor ).data

      
   ### upload Compute data to object storage ###
   #############################################          
   def create_csv(self):
      # Dedicated VM Hosts
      data = 'id, availability_domain, compartment_id, dedicated_vm_host_shape, display_name, fault_domain, lifecycle_state, remaining_ocpus, total_ocpus, report_no'

      for host in self.dedicated_hosts:
         data += '\n'
         data += f'{host.id}, {host.availability_domain}, {host.compartment_id}, {host.dedicated_vm_host_shape}, {host.display_name}, {host.fault_domain}, {host.lifecycle_state}, {host.remaining_ocpus}, {host.total_ocpus}, {report_no}'

      write_file( data, 'dedicated_vm_host' )

      # VM Instances
      data = 'instance_id, availability_domain, compartment_id, dedicated_vm_host_id, display_name, fault_domain, lifecycle_state, region, shape, tenancy_id, report_no'

      for instance in self.instances:
         data += '\n'
         data += f'{instance.id}, {instance.availability_domain}, {instance.compartment_id}, {instance.dedicated_vm_host_id}, {instance.display_name}, {instance.fault_domain}, {instance.lifecycle_state}, {instance.region}, {instance.shape}, {self.tenancy_id}, {report_no}'

      write_file( data, 'instance' )

      # Boot Volume Attachments
      data = 'id, availability_domain, boot_volume_id, compartment_id, display_name, instance_id, is_pv_encryption_in_transit_enabled, lifecycle_state, report_no'

      for bv in self.bv_attachments:
         data += '\n'
         data += f'{bv.id}, {bv.availability_domain}, {bv.boot_volume_id}, {bv.compartment_id}, {bv.display_name}, {bv.instance_id}, {bv.is_pv_encryption_in_transit_enabled}, {bv.lifecycle_state}, {report_no}'

      write_file( data, 'bv_attachment' )

      # Block Volume Attachments
      data = 'id, attachment_type, availability_domain, compartment_id, device, display_name, instance_id, is_pv_encryption_in_transit_enabled, is_read_only, is_shareable, lifecycle_state, volume_id, report_no'
      for vol in self.vol_attachments:
         data += '\n'
         data += f'{vol.id}, {vol.attachment_type}, {vol.availability_domain}, {vol.compartment_id}, {vol.device}, {vol.display_name}, {vol.instance_id}, {vol.is_pv_encryption_in_transit_enabled}, {vol.is_read_only}, {vol.is_shareable}, {vol.lifecycle_state}, {vol.volume_id}, {report_no}'
         write_file( data, 'vol_attachment' )

class BlockStorage(object):
   logger.info("Initiate Block Storage object...")
   
   boot_volumes = []
   block_volumes = []

   def __init__(self, config, tenancy, signer):
      jobs = []
      
      # loop over all regions
      for region in tenancy.regions:
         signer.region = region.region_name
         block_storage_client = oci.core.BlockstorageClient(config, signer=signer)
         
         # loop over all compartments from each region
         for c in tenancy.get_compartments():  
            # initiate a thread for each compartment
            thread = Thread(target = self.get_info, args=(c, block_storage_client, tenancy, region))
            jobs.append(thread)
               
      # start all threads
      for job in jobs:
         job.start()
         
      # join threads so we don't quit until all threads have finished
      for job in jobs:
         job.join()
                  
      logger.debug(" --- List of Block Volumes is --- ")
      logger.debug(self.block_volumes)
      logger.debug(" --- List of Boot Volumes is --- ")
      logger.debug(self.boot_volumes)
      
      logger.info("Block Storage - DONE.")
      
   ### thread function - get all info about block storage ###
   ##########################################################
   def get_info(self, c, block_storage_client, tenancy, region):     
      # get all block volumes
      ads = tenancy.get_availability_domains(region.region_name)
      self.block_volumes += block_storage_client.list_volumes(c.id, retry_strategy=retry_strategy_via_constructor).data
      
      for ad in ads:   
         # get all boot volumes from each AD         
         self.boot_volumes += block_storage_client.list_boot_volumes(ad.name, c.id, retry_strategy=retry_strategy_via_constructor).data
         
   ### upload Block Storage data to object storage ###
   ###################################################      
   def create_csv(self):
      # Boot Volumes
      data = 'id, availability_domain, compartment_id, display_name, image_id, is_hydrated, kms_key_id, lifecycle_state, size_in_gbs, size_in_mbs, volume_group_id, vpus_per_gb, report_no'

      for bv in self.boot_volumes:
         data += '\n'
         data += f'{bv.id}, {bv.availability_domain}, {bv.compartment_id}, {bv.display_name}, {bv.image_id}, {bv.is_hydrated}, {bv.kms_key_id}, {bv.lifecycle_state}, {bv.size_in_gbs}, {bv.size_in_mbs}, {bv.volume_group_id}, {bv.vpus_per_gb}, {report_no}'

      write_file( data, 'boot_volume' )

      # Block Volumes
      data = 'id, availability_domain, compartment_id, display_name, is_hydrated, kms_key_id, lifecycle_state, size_in_gbs, size_in_mbs, volume_group_id, vpus_per_gb, report_no'

      for bv in self.block_volumes:
         data += '\n'
         data += f'{bv.id}, {bv.availability_domain}, {bv.compartment_id}, {bv.display_name}, {bv.is_hydrated}, {bv.kms_key_id}, {bv.lifecycle_state}, {bv.size_in_gbs}, {bv.size_in_mbs}, {bv.volume_group_id}, {bv.vpus_per_gb}, {report_no}'

      write_file( data, 'block_volume' )

class DBSystem(object):
   logger.info("Initiate DB System object...")
   
   db_systems = []
   db_homes = []
   databases = []
   dg_associations = []
   autonomous_exadata = []
   autonomous_cdb = []
   autonomous_db = []

   def __init__(self, config, tenancy, signer):
      jobs = []
      
      # loop over all regions
      for region in tenancy.regions:
         signer.region = region.region_name
         db_client = oci.database.DatabaseClient(config, signer=signer)

         # loop over all compartments from each region
         for c in tenancy.get_compartments():   
            # initiate a thread for each compartment
            thread = Thread(target = self.get_info, args=(c, db_client, tenancy, region))
            jobs.append(thread)
         
      # start all threads
      for job in jobs:
         job.start()
         
      # join threads so we don't quit until all threads have finished
      for job in jobs:
         job.join()
         
      logger.debug(" --- List of DB Systems is --- ")
      logger.debug(self.db_systems)
      logger.debug(" --- List of DB Homes is --- ")
      logger.debug(self.db_homes)
      logger.debug(" --- List of DBs is --- ")
      logger.debug(self.databases)
      logger.debug(" --- List of Autonomous Exadata Infra is --- ")
      logger.debug(self.autonomous_exadata)
      logger.debug(" --- List of Autonomous Container DB is --- ")
      logger.debug(self.autonomous_cdb)
      logger.debug(" --- List of Autonomous DB is --- ")
      logger.debug(self.autonomous_db)
         
      logger.info("DB Systems - DONE.")
               
   ### thread function - get all info about DB Systems ###
   #######################################################
   def get_info(self, c, db_client, tenancy, region):  
      # get all db systems
      self.db_systems += db_client.list_db_systems(c.id, retry_strategy=retry_strategy_via_constructor).data

      # get all db homes
      db_homes = db_client.list_db_homes(c.id, retry_strategy=retry_strategy_via_constructor).data
      self.db_homes += db_homes

      for db_home in db_homes:
         # get all databases from each db home
         self.databases += db_client.list_databases(c.id, db_home_id=db_home.id, retry_strategy=retry_strategy_via_constructor).data
      
      # for db in databases:
      #    self.dg_associations += db_client.list_data_guard_associations(db.id).data             
      
      # get all autonomous exadata infra
      self.autonomous_exadata += db_client.list_autonomous_exadata_infrastructures(c.id, retry_strategy=retry_strategy_via_constructor).data
      # get all autonomous container dbs
      self.autonomous_cdb += db_client.list_autonomous_container_databases(c.id, retry_strategy=retry_strategy_via_constructor).data
      # get all autonomous dbs
      self.autonomous_db += db_client.list_autonomous_databases(c.id, retry_strategy=retry_strategy_via_constructor).data

   ### upload DB Systems data to object storage ###
   ################################################
   def create_csv(self, config):
      self.tenancy_id = config["tenancy"]
      
      # DB System
      data = 'id, availability_domain, cluster_name, compartment_id, cpu_core_count, data_storage_percentage, data_storage_size_in_gbs, database_edition, disk_redundancy, display_name, domain, hostname, lifecycle_state, node_count, reco_storage_size_in_gb, shape, sparse_diskgroup, version, region_id, tenancy_id, report_no'

      for db_system in self.db_systems:
         region_id = db_system.id.split(".")[3]
         data += '\n'
         data += f'{db_system.id}, {db_system.availability_domain}, {db_system.cluster_name}, {db_system.compartment_id}, {db_system.cpu_core_count}, {db_system.data_storage_percentage}, {db_system.data_storage_size_in_gbs}, {db_system.database_edition}, {db_system.disk_redundancy}, {db_system.display_name}, {db_system.domain}, {db_system.hostname}, {db_system.lifecycle_state}, {db_system.node_count}, {db_system.reco_storage_size_in_gb}, {db_system.shape}, {db_system.sparse_diskgroup}, {db_system.version}, {region_id}, {self.tenancy_id}, {report_no}'
      
      write_file( data, 'db_system' )

      # DB Home
      data = 'id, compartment_id, db_system_id, db_version, display_name, last_patch_history_entry_id, lifecycle_state, report_no'

      for db_home in self.db_homes:
         data += '\n'
         data += f'{db_home.id}, {db_home.compartment_id}, {db_home.db_system_id}, {db_home.db_version}, {db_home.display_name}, {db_home.last_patch_history_entry_id}, {db_home.lifecycle_state}, {report_no}'

      write_file( data, 'db_home' )
      
      # Database
      data = 'id, compartment_id, auto_backup_enabled, auto_backup_window, backup_destination_details, recovery_window_in_days, db_home_id, db_name, db_unique_name, db_workload, lifecycle_state, pdb_name, report_no'

      for db in self.databases:
         data += '\n'
         db_auto_backup_enabled = 'False' if db.db_backup_config == None else db.db_backup_config.auto_backup_enabled
         db_auto_backup_window  = 'None' if db.db_backup_config == None else {db.db_backup_config.auto_backup_window}
         db_backup_destination_details  = 'None' if db.db_backup_config == None else {db.db_backup_config.backup_destination_details}
         db_recovery_window_in_days = 'None' if db.db_backup_config == None else {db.db_backup_config.recovery_window_in_days}

         data += f'{db.id}, {db.compartment_id}, {db_auto_backup_enabled}, {db_auto_backup_window}, {db_backup_destination_details}, {db_recovery_window_in_days}, {db.db_home_id}, {db.db_name}, {db.db_unique_name}, {db.db_workload}, {db.lifecycle_state}, {db.pdb_name}, {report_no}'

      write_file( data, 'database' )

      # DG Association
      # for db in self.databases:
      #    create_csv( f'' )

      # Autonomous Exadata
      data = 'id, availability_domain, compartment_id, display_name, domain, hostname, last_maintenance_run_id, license_model, lifecycle_state, maintenance_window, next_maintenance_run_id, shape, report_no'

      for auto_exadata in self.autonomous_exadata:
         data += '\n'
         data += f'{auto_exadata.id}, {auto_exadata.availability_domain}, {auto_exadata.compartment_id}, {auto_exadata.display_name}, {auto_exadata.domain}, {auto_exadata.hostname}, {auto_exadata.last_maintenance_run_id}, {auto_exadata.license_model}, {auto_exadata.lifecycle_state}, {auto_exadata.maintenance_window}, {auto_exadata.next_maintenance_run_id}, {auto_exadata.shape}, {report_no}'

      write_file( data, 'autonomous_exadata' )

      # Autonomous Container DB
      data = 'id, autonomous_exadata_infrastructure_id, availability_domain, backup_config, compartment_id, display_name, last_maintenance_run_id, lifecycle_state, maintenance_window, next_maintenance_run_id, patch_model, service_level_agreement_type, report_no'

      for acdb in self.autonomous_cdb:
         data += '\n'
         data += f'{acdb.id}, {acdb.autonomous_exadata_infrastructure_id}, {acdb.availability_domain}, {acdb.backup_config}, {acdb.compartment_id}, {acdb.display_name}, {acdb.last_maintenance_run_id}, {acdb.lifecycle_state}, {acdb.maintenance_window}, {acdb.next_maintenance_run_id}, {acdb.patch_model}, {acdb.service_level_agreement_type}, {report_no}'

      write_file( data, 'autonomous_cdb' )

      # Autonomous DB     
      data = 'id, autonomous_container_database_id, compartment_id, cpu_core_count, data_safe_status, data_storage_size_in_tbs, db_name, db_version, db_workload, display_name, is_auto_scaling_enabled, is_dedicated, is_free_tier, lifecycle_state, whitelisted_ips, report_no'
      
      for adb in self.autonomous_db:
         data += '\n'
         data += f'{adb.id}, {adb.autonomous_container_database_id}, {adb.compartment_id}, {adb.cpu_core_count}, {adb.data_safe_status},  {adb.data_storage_size_in_tbs}, {adb.db_name}, {adb.db_version}, {adb.db_workload}, {adb.display_name}, {adb.is_auto_scaling_enabled}, {adb.is_dedicated}, {adb.is_free_tier}, {adb.lifecycle_state}, {adb.whitelisted_ips}, {report_no}'

      write_file( data, 'autonomous_db' )


class Monitoring(object):
   logger.info("Initiate Monitoring object...")
    
   compute_metrics_data = []
   autonomous_metrics_data = []

   def __init__(self, config, tenancy, signer):      
      jobs = []
      compute_metrics_list = [ ( 'CpuUtilization', 'mean' ),  ( 'MemoryUtilization', 'mean' ), ( 'DiskBytesRead', 'rate' ), ( 'DiskBytesWritten', 'rate' ), ( 'NetworksBytesIn', 'rate' ), ( 'NetworksBytesOut', 'rate' ) ]
      autonomous_metrics_list = [ ( 'CpuUtilization', 'mean' ),  ( 'StorageUtilization', 'mean' ), ('CurrentLogons', 'sum')]

      # loop over each region in the tenancy
      for region in tenancy.regions:
         signer.region = region.region_name
         monitor = oci.monitoring.MonitoringClient(config, signer=signer)
         start_time = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime('%Y-%m-%dT00:00:00.000Z')
         end_time = datetime.datetime.today().strftime('%Y-%m-%dT00:00:00.000Z')   
         
         # loop over the metrics in the compute_metrics_list
         for metric in compute_metrics_list:
            metrics_summary = oci.monitoring.models.SummarizeMetricsDataDetails( end_time=end_time, namespace='oci_computeagent', query=f'{metric[0]}[1m].{metric[1]}()', start_time=start_time)
            
            # initiate a thread for each metric
            thread = Thread(target = self.get_metrics_compute, args=(region, config, monitor, metrics_summary))
            jobs.append(thread)
            
         # loop over the metrics in the autonomous_metrics_list
         for metric in autonomous_metrics_list:
            metrics_summary = oci.monitoring.models.SummarizeMetricsDataDetails( end_time=end_time, namespace='oci_autonomous_database', query=f'{metric[0]}[1m].{metric[1]}()', start_time=start_time)
            
            # initiate a thread for each metric
            thread = Thread(target = self.get_metrics_autonomous, args=(region, config, monitor, metrics_summary))
            jobs.append(thread)
            
            
      # start all threads
      for job in jobs:
         job.start()
         
      # join threads so we don't quit until all threads have finished
      for job in jobs:
         job.join()
         
         
   ### thread function - get all info about Compute Metrics ###
   #######################################################
   def get_metrics_compute(self, region, config, monitor, metrics_summary):  
      self.compute_metrics_data += monitor.summarize_metrics_data( config[ "tenancy" ], metrics_summary, compartment_id_in_subtree=True, retry_strategy=retry_strategy_via_constructor).data
   
   ### thread function - get all info about Compute Metrics ###
   #######################################################
   def get_metrics_autonomous(self, region, config, monitor, metrics_summary):  
       self.autonomous_metrics_data += monitor.summarize_metrics_data( config[ "tenancy" ], metrics_summary, compartment_id_in_subtree=True, retry_strategy=retry_strategy_via_constructor).data
      
   ### upload Metrics data to object storage ###
   ################################################
   def create_csv(self, config):
      self.tenancy_id = config["tenancy"]
      
      # write data for Compute Metrics
      data = 'metric_name, resource_id, timestamp, value, tenancy_id, report_no'

      for metrics in self.compute_metrics_data:
         for datapoint in metrics.aggregated_datapoints:
            data += '\n'
            data += f'{metrics.name}, {metrics.dimensions[ "resourceId" ]}, {str(datapoint.timestamp)}, {datapoint.value}, {self.tenancy_id}, {report_no}'

      write_file( data, 'metrics_compute' )
      
      # write data for Autonomous DB Metrics
      data = 'metric_name, resource_id, timestamp, value, tenancy_id, report_no'

      for metrics in self.autonomous_metrics_data:
         for datapoint in metrics.aggregated_datapoints:
            data += '\n'
            data += f'{metrics.name}, {metrics.dimensions[ "resourceId" ]}, {str(datapoint.timestamp)}, {datapoint.value}, {self.tenancy_id}, {report_no}'

      write_file( data, 'metrics_autonomous_db' )


### Upload data to Object Storage ###
#####################################
def write_file( strdata, filename ):
   global report_no
   global par_url

   try:
      resp = requests.put( f'{par_url}{filename}_{report_no}.csv', data=strdata.encode('utf-8'))
      logger.info(f'Uploading file: {par_url}{filename}_{report_no}.csv to object storage.')
   except Exception as err:
      logger.error( f'Failed to upload file : {filename}_{report_no}')
      logger.error(err)
