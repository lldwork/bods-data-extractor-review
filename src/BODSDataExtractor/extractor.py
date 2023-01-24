# =============================================================================
# This examples file is designed to show an example use case of this library,
# and be executed in one go.
# In this example, we are interested in 2 particular noc codes (BPTR and RBTS),
# that have been flagged as in need of further investigation from previous manual analysis.
# 
# All that is needed is to enter your api key in the variable 'api', and run the code!
# =============================================================================

#try:
from BODSDataExtractor.extractor import TimetableExtractor
#except:
#  from extractor import TimetableExtractor
  
# LLD add a check to prevent difficult to trace errors - use isalnum() to check api is alphanumeric
# and that it isn't equal to the original phrase. if it is, stop with error telling user
# to ensure api key is set  
api = 'c82016fb18cacbfafc4920b526bd72b3520f3ceb'

#-------------------------------------------
#            FINE TUNED RESULTS
#-------------------------------------------
#intiate an object instance called my_bus_data_object with desired parameters 

# LLD the instantiator should not carry out the functionality itself.
# separate this functionality to call the instantior and then
# run (from this file) e.g. a get_timetables() method. Do not include analysis within
# this method - that should be separate, to allow the user to choose which bits of
# functionality they'd want to use and avoid unnecessary overhead.

my_bus_data_object = TimetableExtractor(api_key=api
                                 ,status = 'published' 
                                 ,service_line_level=True 
                                 ,stop_level=True 
                                 ,nocs=['BPTR','RBTS']
                                 ,bods_compliant=True
                                 )

# LLD dataset doesn't mean a lot - what will this mean in layman's terms?
#save the extracted dataset level data to filtered_dataset_level variable
filtered_dataset_level = my_bus_data_object.metadata

#save the extracted dataset level data to lcoal csv file
my_bus_data_object.save_metadata_to_csv()

#save the extracted service line level data to dataset_level variable
filtered_service_line_level = my_bus_data_object.service_line_extract

# LLD method names should be as short as possible - e.g. service_lines_to_csv
#save the extracted service line level data to lcoal csv file
my_bus_data_object.save_service_line_extract_to_csv()

#save the extracted stop level data to stop_level variable
stop_level = my_bus_data_object.timetable_dict

# LLD this should have the otpion to save as multiple csv or as one big csv.
# equally it should be easy for user to combine the dictionary of dataframes into one df
# (using whatever unique ID is used for files as the ID column in the df).
# This will make it far easier for user to complete analysis over the whole
# population
#stop_level variable is a dictionary of dataframes, which can be saved to csv as follows (saves in downloads folder)
my_bus_data_object.save_all_timetables_to_csv()




#-------------------------------------------
#       REPORTING / ANALYTICS
#-------------------------------------------

# LLD I would suggest that all the below are combined into one function that prints out a df - 
# run_DQ_report()) for example. I would not label this as analytics - really we don't need to include
# analytics as the potential functionality could be infinite. Just need to make sure the data is supplied
# in the best possible format for the user to begin their own analysis.
count_of_operators = my_bus_data_object.count_operators() #returns count of distinct operators (measured by operator_name) in a chosen dataset

count_of_service_codes = my_bus_data_object.count_service_codes()# returns count of unique service codes chosen dataset

valid_service_codes = my_bus_data_object.valid_service_codes()# returns count of unique and valid service codes chosen dataset, a dataframe with all the records with valid service codes and a dataframe with all the invalid service codes.

services_published_in_TXC_2_4 = my_bus_data_object.services_published_in_TXC_2_4()#returns percentage of services published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records that are not published in this schema

datasets_published_in_TXC_2_4 = my_bus_data_object.datasets_published_in_TXC_2_4()# returns percentage of datasets published in TXC 2.4 schema, and a dataframe of these records, and a dataframe of the records that are not published in this schema

red_dq = my_bus_data_object.red_dq_scores() #returns the number of operators in a table with red dq scores

less_than_10 = my_bus_data_object.dq_less_than_x(90) # takes an integer as input (in this case 10) and returns a list of operators with a data quality score less than that integer

no_lic_no = my_bus_data_object.no_licence_no() # returns a report listing which datasets contain files which do not have a licence number

