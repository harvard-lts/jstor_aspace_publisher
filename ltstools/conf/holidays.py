# Days seen as Harvard holidays by our scripts. Updating the lists
# between Thanksgiving and winter break is probably a good idea.

# Update this when updating the lists below. 
# Set it later than the last holiday listed but before the next one coming.
updatedTil = '20231218'

winterBreak = ['20221223', '20221224', '20221225', '20221226', '20221227', 
               '20221228', '20221229', '20221230', '20221231', '20230101', 
               '20230102']

# The winterBreak list above is added to the holiday list
holidays = ['20230116', '20230220', '20230529', '20230619', 
			'20230704', '20230904', '20231009', '20231110', '20231123', 
			'20231124']

for datestamp in winterBreak:
	holidays.append(datestamp)
