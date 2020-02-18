import os
import time
import datetime

import din_parser as din        # DIN Parser module
import iso_parser as iso        # ISO Parser module
import rshd_parser as rshd      # RSHD Parser module
import palert_parser as palert  # Palert Parser module

import functions
import archiving
import notification
import uploader

import config

iso_dir_path = config.data_path + 'ISOfiles'  # jentest'
din_dir_path = config.data_path + 'DINfiles'  # jentest'
rshd_dir_path = config.data_path + 'RSHD'  # jentest'
divider = '/'

year = datetime.datetime.now().year  # Current Year
bucket = config.bucket  # AWS S3 Bucket Name

# Error Message
error_no_device = 'There is no device: '
error_storing = 'Error during storing: '

output_filename = ''

############## RSHD, Palert PARSER ##############

# Seek RSHD path
for (path, dir, files) in os.walk(rshd_dir_path):
    for f in files:
        error = False
        error_message = ''

        # IF there is ZIP files, delete them
        # if f.find('.zip') > -1:
        # os.remove(path + divider + f)
        if "archving" not in path:
            if "error" not in path:

                # RSHD file full path
                # e.g.) ~/loaddata/RSHD/C10129
                fullpath = path + divider + f

                # RSHD CSV Parsing
                # e.g) RSHDEVENT_C10129_20190903_223209.csv
                if f.find('RSHDEVENT') > -1:
                    functions.log('############# RSHD #############')
                    rshd_id = 0

                    try:
                        # Get File Info from Filename
                        fileInfos = rshd.parseFileName(fullpath)
                        functions.log("%s" % (fullpath))
                        functions.log(fileInfos)
                        # e.g.) ['../loaddata/RSHD/C10129/RSHDEVENT', 'C10129', '20190903', '223209']

                        # Get Date and Time from inside the file
                        datetime = rshd.getDatetime(fullpath)

                        # Get total count of the RSHD data (excluding RSHD RESULT)
                        count = rshd.countCSV(fullpath)
                        functions.log("count: %s" % (count))

                        # Get Device ID in the web application
                        device_id = functions.getDeviceID(fileInfos[1])
                        functions.log("device_id: %s" % (device_id))

                        # If there is no device
                        if(device_id is None):
                            error = True
                            error_message = error_no_device
                        else:
                            # Upload the file to AWS S3
                            # @ param: filetype, filepath, serial, dateinfo, filename, bucket, divider
                            # @ return: ZIP file full path
                            output_filename = uploader.uploadToS3(
                                'RSHD', fullpath, fileInfos[1], datetime[0], f, bucket, divider)

                            # Insert RSHD result to DB
                            # @ param: file_path, device_id, datetime, count
                            rshd_id = rshd.rshd_result(
                                fullpath, device_id, datetime, count)
                            functions.log("DB rshd_id: %s" % (rshd_id))

                            # If the storing is fail
                            if(rshd_id < 1):
                                error = True
                                error_message = error_storing

                        # Delete CSV file
                        # os.remove(fullpath)

                    except Exception as e:
                        error = True
                        error_message = e
                        functions.log(e)

                    finally:
                        # Error Handling
                        if(error):
                            # Move to error directory
                            archiving.backup_error_file(path + divider, f)

                            # Send email to engineer
                            notification.sendEmail(error_message, fullpath)

                            # Delete the row in RSHD table
                            if(rshd_id > 0):
                                rshd.rollbackDB(rshd_id)
                        else:
                            archiving.move_archiving(path + divider, f)

                        # Delete ZIP file
                        if os.path.exists(output_filename):
                            os.remove(output_filename)

                # Palert CSV Parsing
                # e.g) _palert_3836_eqEvent_20191015022441.csv
                elif f.find('eqEvent') > -1:
                    functions.log('####### Palert (eqEvent) #######')
                    p_id = 0

                    try:
                        # Get File Info from Filename
                        fileInfos = palert.parseFileNameForEqEvent(fullpath)
                        functions.log("%s" % (fullpath))
                        functions.log(fileInfos)
                        # e.g.) ['', 'paler', '3836', 'eqEvent', '20191015022441.csv', '', '']

                        # Get Date and Time from inside the file
                        datetime_deviceid = palert.getDatetimeAndDeviceId(
                            fullpath)
                        functions.log("datetime_deviceid: %s" %
                                      (datetime_deviceid))

                        # Get total count of the Palert data (excluding Overall Information)
                        count = palert.countCSV(fullpath, 14)
                        functions.log("count: %s" % (count))

                        # Get Device ID in the web application
                        device_id = functions.getDeviceID(datetime_deviceid[2])
                        functions.log("device_id: %s" % (device_id))

                        # If there is no device
                        if(device_id is None):
                            error = True
                            error_message = error_no_device
                        else:
                            # Upload the file to AWS S3
                            # @ param: filetype, filepath, serial, dateinfo, filename, bucket, divider
                            # @ return: ZIP file full path
                            output_filename = uploader.uploadToS3(
                                'Palert', fullpath, datetime_deviceid[2], fileInfos[5], f, bucket, divider)

                            # Insert RSHD result to DB
                            p_id = palert.storeDB(
                                datetime_deviceid[0], datetime_deviceid[1], device_id, 'E', count, fileInfos[4])
                            functions.log("DB p_id: %s" % (p_id))

                            # If the storing is fail
                            if(p_id < 1):
                                error = True
                                error_message = error_storing

                        # Delete CSV file
                        # os.remove(fullpath)

                    except Exception as e:
                        error = True
                        error_message = e
                        functions.log(e)

                    finally:
                        # Error Handling
                        if(error):
                            # Move to error directory
                            archiving.backup_error_file(path + divider, f)

                            # Send email to engineer
                            notification.sendEmail(error_message, fullpath)

                            # Delete the row in Palert table
                            if(p_id > 0):
                                palert.rollbackDB(p_id)
                        else:
                            archiving.move_archiving(path + divider, f)

                        # Delete ZIP file
                        if os.path.exists(output_filename):
                            os.remove(output_filename)

                # Palert CSV Parsing
                # e.g) 20190916173301_3846.csv
                elif(int(f[0:4]) == year):
                    functions.log('############# Palert #############')
                    p_id = 0

                    try:
                        # Get File Info from Filename
                        fileInfos = palert.parseFileName(fullpath)
                        functions.log("%s" % (fullpath))
                        functions.log(fileInfos)
                        # e.g.) ['20190916173301', '3846', '20190916', '173301']

                        # Get Date and Time from inside the file
                        datetime_deviceid = palert.getDatetimeAndDeviceId(
                            fullpath)
                        functions.log("datetime_deviceid: %s" %
                                      (datetime_deviceid))

                        # Get total count of the Palert data (excluding Overall Information)
                        count = palert.countCSV(fullpath, 17)
                        functions.log("count: %s" % (count))

                        # Get Device ID in the web application
                        device_id = functions.getDeviceID(datetime_deviceid[2])
                        functions.log("device_id: %s" % (device_id))

                        # If there is no device
                        if(device_id is None):
                            error = True
                            error_message = error_no_device
                        else:
                            # Upload the file to AWS S3
                            # @ param: filetype, filepath, serial, dateinfo, filename, bucket, divider
                            # @ return: ZIP file full path
                            output_filename = uploader.uploadToS3(
                                'Palert', fullpath, datetime_deviceid[2], fileInfos[2], f, bucket, divider)

                            # Insert RSHD result to DB
                            p_id = palert.storeDB(
                                datetime_deviceid[0], datetime_deviceid[1], device_id, 'N', count, fileInfos[0])
                            functions.log("DB p_id: %s" % (p_id))

                            # If the storing is fail
                            if(p_id < 1):
                                error = True
                                error_message = error_storing

                        # Delete CSV file
                        # os.remove(fullpath)

                    except Exception as e:
                        error = True
                        error_message = e
                        functions.log(e)

                    finally:
                        # Error Handling
                        if(error):
                            # Move to error directory
                            archiving.backup_error_file(path + divider, f)

                            # Send email to engineer
                            notification.sendEmail(error_message, fullpath)

                            # Delete the row in Palert table
                            if(p_id > 0):
                                palert.rollbackDB(p_id)
                        else:
                            archiving.move_archiving(path + divider, f)

                        # Delete ZIP file
                        if os.path.exists(output_filename):
                            os.remove(output_filename)

############## ISO PARSER ##############

# Seek ISO path
for (path, dir, files) in os.walk(iso_dir_path):
    for f in files:
        error = False
        error_message = ''

        # IF there is ZIP files, delete them
        # if f.find('.zip') > -1:
        #     os.remove(path + divider + f)
        if "archving" not in path:
            if "error" not in path:

                # ISO file full path
                # e.g.) ~/loaddata/ISOfiles/3851
                fullpath = path + divider + f

                # ISO CSV Parsing
                # e.g) _palert_3851_iso2631Daily_20190415.csv
                if f.find('iso2631Daily') > -1:
                    functions.log('############# ISO #############')
                    iso_id = 0

                    # Get File Info from Filename
                    fileInfos = iso.parseFileName(fullpath)
                    functions.log("%s" % (fullpath))
                    functions.log(fileInfos)
                    # ['../loaddata/ISOfiles/3851/', 'palert', '3851', 'iso2631Daily', '20190420']

                    # Get total count of the ISO data
                    result = iso.countCSV(fullpath, fileInfos[2])
                    count = result[0]
                    status = result[1]
                    functions.log("count: %s" % (count))

                    if(count < 80000):
                        functions.log(
                            "The amount of data is smaller than 80,0000")
                    else:
                        try:
                            # Get Device ID in the web application
                            device_id = functions.getDeviceID(fileInfos[2])
                            functions.log("device_id: %s" % (device_id))

                            # If there is no device
                            if(device_id is None):
                                error = True
                                error_message = error_no_device
                            else:
                                # Upload the file to AWS S3
                                # @ param: filetype, filepath, serial, dateinfo, filename, bucket, divider
                                # @ return: ZIP file full path
                                output_filename = uploader.uploadToS3(
                                    'ISO', fullpath, fileInfos[2], fileInfos[4], f, bucket, divider)

                                # Insert ISO result to DB
                                # @ param: file_path, device_id, datetime, count
                                iso_id = iso.storeDB(
                                    fileInfos[4], device_id, count, status)
                                functions.log("DB iso_id: %s" % (iso_id))

                                # If the storing is fail
                                if(iso_id < 1):
                                    error = True
                                    error_message = error_storing

                            # Delete CSV file
                            # os.remove(fullpath)

                        except Exception as e:
                            error = True
                            error_message = e
                            functions.log(e)

                        finally:
                            # Error Handling
                            if(error):
                                # Move to error directory
                                archiving.backup_error_file(path + divider, f)

                                # Send email to engineer
                                notification.sendEmail(error_message, fullpath)

                                # Delete the row in ISO table
                                if(iso_id > 0):
                                    iso.rollbackDB(iso_id)
                            else:
                                archiving.move_archiving(path + divider, f)

                            # Delete ZIP file
                            if os.path.exists(output_filename):
                                os.remove(output_filename)

                            # if(status < 1):
                            #     notification.insertSMS('I', fileInfos[2], fileInfos[4])

############## DIN PARSER ##############

# Seek DIN path
for (path, dir, files) in os.walk(din_dir_path):
    for f in files:
        error = False
        error_message = ''

        # IF there is ZIP files, delete them
        # if f.find('.zip') > -1:
        #     os.remove(path + divider + f)
        if "archving" not in path:
            if "error" not in path:

                # DIN file full path
                # e.g.) ~/loaddata/DINfiles/1294
                fullpath = path + divider + f

                # DIN CSV Parsing
                # e.g) DIN20190613_1294.csv
                if(f.find('DIN') > -1):
                    functions.log('############# DIN #############')
                    din_id = 0

                    # Get File Info from Filename
                    fileInfos = din.parseFileName(fullpath)
                    functions.log("%s" % (fullpath))
                    functions.log(fileInfos)
                    # ['files/1294/DIN20190613', '1294', '20190613']

                    # Get total count of the DIN data
                    count = din.countCSV(fullpath)
                    functions.log("count: %s" % (count))

                    # if(count < 80000):
                    #     functions.log(
                    #         "The amount of data is smaller than 80,0000")

                    # else:
                    try:
                        # Get Device ID in the web application
                        device_id = functions.getDeviceID(fileInfos[1])
                        functions.log("device_id: %s" % (device_id))

                        # If there is no device
                        if(device_id is None):
                            error = True
                            error_message = error_no_device
                        else:
                            # Upload the file to AWS S3
                            # @ param: filetype, filepath, serial, dateinfo, filename, bucket, divider
                            # @ return: ZIP file full path
                            output_filename = uploader.uploadToS3(
                                'DIN', fullpath, fileInfos[1], fileInfos[2], f, bucket, divider)

                            # Insert DIN result to DB
                            # @ param: file_path, device_id, datetime, count
                            din_id = din.storeDB(
                                fileInfos[2], device_id, count)
                            functions.log("din_id: %s" % (din_id))

                            # If the storing is fail
                            if(din_id < 1):
                                error = True
                                error_message = error_storing

                        # Delete CSV file
                        # os.remove(fullpath)

                    except Exception as e:
                        error = True
                        error_message = e
                        functions.log(e)

                    finally:
                        # Error Handling
                        if(error):
                            # Move to error directory
                            archiving.backup_error_file(path + divider, f)

                            # Send email to engineer
                            notification.sendEmail(error_message, fullpath)

                            # Delete the row in DIN table
                            if(din_id > 0):
                                din.rollbackDB(din_id)
                        else:
                            archiving.move_archiving(path + divider, f)

                        # Delete ZIP file
                        if os.path.exists(output_filename):
                            os.remove(output_filename)
