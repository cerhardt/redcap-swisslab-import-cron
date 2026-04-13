<?php

define('WORK_DIR',dirname(__FILE__));
require_once (dirname(__FILE__)."/../../init.php");

//======================================================================================================================================
// Import Lab Results
$aFiles = array_slice(scandir(DATA_DIR), 2);
$aLabResults = array();

// Read whitelist for text results
$aWhitelist = array();
$fp = @fopen("resultat_ergebnissest_whitelist.txt", "r");
if ($fp) {
    while (($buffer = fgets($fp, 4096)) !== false) {
        $aWhitelist[trim($buffer)] = true;
    }
    fclose($fp);
}

if (is_array($aFiles)) {
    foreach($aFiles as $file) {
        $aTmp=explode(".",$file);
        $suf = strtolower($aTmp[count($aTmp)-1]);
        if ($file != "." && $file != ".." && $suf == 'json') {
            $handle2 = fopen(DATA_DIR."/".$file, "r");
            if ($handle2) {
                
                $buffer = fgets($handle2);
                $aDataBlock = json_decode($buffer,true);
                fclose($handle2);
                
                // delete file
                //unlink(DATA_DIR."/".$file);
                
                // extract IDs
                $iISH_ID = trim(ltrim($aDataBlock['metadata']['patient']['identifier'][0]['value'],'0'));
                $iFallnr = trim(ltrim($aDataBlock['metadata']['additionalResources']['entry'][0]['resource']['identifier'][0]['value'],'0'));

                // skip if IDs are empty
                if (strlen($iFallnr) == 0 || strlen($iISH_ID) == 0) {
                    continue;
                }

                if (is_array($aDataBlock['payload'])) {

                    // date: Abnahmedatum or Auftragsdatum
                    if (!isset($aDataBlock['payload']['abnahmeDatum']) || strlen($aDataBlock['payload']['abnahmeDatum']) == 0) {
                        $sDatum = $aDataBlock['payload']['auftragDatum'];
                    } else {
                        $sDatum = $aDataBlock['payload']['abnahmeDatum'];
                    }
                                        
                    foreach($aDataBlock['payload']['resultate'] as $aResults) {

                         if (!isset($aResults['ergebnisZahl'])) {
                             // text results
                             if (isset($aResults['ergebnis']) && strlen(trim($aResults['ergebnis'])) > 0) {
                                if (isset($aWhitelist[trim($aResults['ergebnis'])])) {
                                    $aLabResults[$iISH_ID][$iFallnr][trim($aResults['analyt']['code'])][$sDatum]['resultText'] = trim($aResults['ergebnis']);
                                }
                             }
                             continue;
                         }
                         
                        // if result is empty => get "weiteresResultat"
                        if (strlen(trim($aResults['ergebnisZahl'])) == 0) {
                            if (isset($aResults['weiteresResultat']) && is_array($aResults['weiteresResultat'])) {
                                $aResults = $aResults['weiteresResultat'];
                                if (!isset($aResults['ergebnisZahl']) || strlen(trim($aResults['ergebnisZahl'])) == 0) {
                                    continue;
                                }
                            } else {
                                continue;
                            }
                        }                    

                        // result
                        $aLabResults[$iISH_ID][$iFallnr][trim($aResults['analyt']['code'])][$sDatum]['result'] = trim($aResults['ergebnisZahl']);
                        // unit
                        if (isset($aResults['analyt']['einheitAlt'])) {
                            $aLabResults[$iISH_ID][$iFallnr][trim($aResults['analyt']['code'])][$sDatum]['unit'] = trim($aResults['analyt']['einheitAlt']);
                        }
                        // range
                        if (isset($aResults['normwert']['untergrenzeNormalbereichZahl']) && isset($aResults['normwert']['obergrenzeNormalbereichZahl'])) {
                            $aLabResults[$iISH_ID][$iFallnr][trim($aResults['analyt']['code'])][$sDatum]['range'] = trim($aResults['normwert']['untergrenzeNormalbereichZahl'])."-".trim($aResults['normwert']['obergrenzeNormalbereichZahl']);
                        } elseif (isset($aResults['normwert']['obergrenzeNormalbereichZahl'])) {
                            $aLabResults[$iISH_ID][$iFallnr][trim($aResults['analyt']['code'])][$sDatum]['range'] = "<".trim($aResults['normwert']['obergrenzeNormalbereichZahl']);
                        } elseif (isset($aResults['normwert']['untergrenzeNormalbereichZahl'])) {
                            $aLabResults[$iISH_ID][$iFallnr][trim($aResults['analyt']['code'])][$sDatum]['range'] = ">".trim($aResults['normwert']['untergrenzeNormalbereichZahl']);
                        }

                    }
                }
                    
            } // end if handle
        } // end if .txt
    } // end foreach files
} // end if is array files

//======================================================================================================================================

// get project configs
$request = new RestCallRequest("https://".$GLOBALS['CONFIG']['server_name']."/api/?type=module&prefix=swisslab_import&page=swisslab_config_api&NOAUTH");
$request->setUsername($GLOBALS['CONFIG']['swisslab_config_user']);
$request->setPassword($GLOBALS['CONFIG']['swisslab_config_pw']);
$request->execute(); 
$aConfig = json_decode($request->getResponseBody(),true);

// no data but active projects => send email 
if (count($aLabResults) == 0 && is_array($aConfig)) {
   mail(LOG_EMAIL, 'Swisslab Import '.IMPORT_TARGET.': missing data for import', '', "From: ".gethostname(), "-f ".gethostname());        
}

// no files -> return
if (count($aLabResults) == 0) return;

// get LOINC mappings for ProVal
$aProvalLOINC_csv = hih::csv_to_array("proval_loinc.csv",";");
$aProvalLOINC = array();
foreach($aProvalLOINC_csv as $aTmp) {
    $aProvalLOINC[$aTmp['redcap_field']] = $aTmp['LOINC_Code'];
}

// mapping of date formats to import format
$aDateMapping = array(
    'date_dmy' => 'Y-m-d',
    'date_mdy' => 'Y-m-d',
    'date_ymd' => 'Y-m-d',
    'datetime_dmy' => 'Y-m-d H:i',
    'datetime_mdy' => 'Y-m-d H:i',
    'datetime_ymd' => 'Y-m-d H:i',
    'datetime_seconds_dmy' => 'Y-m-d H:i:s',
    'datetime_seconds_mdy' => 'Y-m-d H:i:s',
    'datetime_seconds_ymd' => 'Y-m-d H:i:s'
);

if (is_array($aConfig)) {
    foreach($aConfig as $iProj => $aProjConfig) {
        $script_start = microtime(true);
        $sLog = '';

        // skip if API token does not exist
        if (!isset($GLOBALS['CONFIG']['swisslab_token_'.$iProj])) {
            continue;
        }
        $sAPIToken = $GLOBALS['CONFIG']['swisslab_token_'.$iProj];

        // project logging: full or incremental
        if (!isset($aProjConfig['full_import']) || $aProjConfig['full_import'] == 'false') {        
            $handle = fopen(dirname(__FILE__).'/logs/swisslab_import_pid'.$iProj.'.txt', "a");
        } else {
            $handle = fopen(dirname(__FILE__).'/logs/swisslab_import_pid'.$iProj.'.txt', "w");
        }

        // get REDCap fields from config
        $aREDCapFieldsK = $aDateFields = array();
        if (isset($aProjConfig['ish_id']) && strlen($aProjConfig['ish_id']) > 0) {
            $aREDCapFieldsK[$aProjConfig['ish_id']] = true;
        }
        if (isset($aProjConfig['case_id']) && strlen($aProjConfig['case_id']) > 0) {
            $aREDCapFieldsK[$aProjConfig['case_id']] = true;
        }

        foreach($aProjConfig['labcodes'] as $aLab) {
            foreach($aLab as $sKey => $aVal) {
                if (strlen($sKey) == 0 || strlen($aVal['redcap_field']) == 0) continue; 
                
                $aREDCapFieldsK[$aVal['redcap_field']] = true;
                if (strlen($aVal['redcap_lab_date']) > 0) {
                    $aREDCapFieldsK[$aVal['redcap_lab_date']] = true;
                    $aDateFields[$aVal['redcap_lab_date']] = true;
                }
                if (strlen($aVal['redcap_visit_date']) > 0) {
                    $aREDCapFieldsK[$aVal['redcap_visit_date']] = true;
                }
            }
        }        
        $aREDCapFields = array_keys($aREDCapFieldsK);
 
        // get data dictionary
        $aREDCapMeta = hih::GetRedcapMetadata($sAPIToken);
        
        // check if fields exist in data dictionary
        foreach($aREDCapFields as $i => $sFieldTmp) {
            if (!isset($aREDCapMeta[$sFieldTmp])) {
                $sLog .= 'Field "'.$sFieldTmp.'" does not exist!'."\n";
                unset ($aREDCapFields[$i]);
            }
        }
        
        // add record_id
        $sPKREDCap = key($aREDCapMeta);
        array_unshift($aREDCapFields,$sPKREDCap);

        // get REDCap events
        $aREDCapEvents = hih::GetRedcapEvents($sAPIToken);
        $aEvents = array();
        if (!isset($aREDCapEvents['error'])) {
            foreach($aREDCapEvents as $aEventTmp) {
                $aEvents[$aEventTmp['event_id']] = $aEventTmp['unique_event_name'];
            }
        } else {
            $sLog .= 'GetRedcapEvents Error:'.$aREDCapEvents['error']."\n";
        }
        
        // get REDCap forms/events mapping
        $aREDCapFormEvents = hih::GetRedcapFormsEvents($sAPIToken);
        $aFormEvents = array();
        if (!isset($aREDCapFormEvents['error'])) {
            foreach($aREDCapFormEvents as $aEventTmp) {
                $aFormEvents[$aEventTmp['form']][] = $aEventTmp['unique_event_name'];
            }
        } else {
            $sLog .= 'GetRedcapFormsEvents Error:'.$aREDCapFormEvents['error']."\n";
        }

        // get REDCap repeating forms
        $aREDCapRepeatingFormEvents = hih::GetRedcapRepeatingFormsEvents($sAPIToken);
        $aRepeatingFormEvents = array();
        if (!isset($aREDCapRepeatingFormEvents['error'])) {
            foreach($aREDCapRepeatingFormEvents as $aFormTmp) {
                // classic projects with repeating forms
                if (!isset($aFormTmp['event_name'])) $aFormTmp['event_name'] = '';
                $aRepeatingFormEvents[$aFormTmp['event_name']][$aFormTmp['form_name']] = true;
            }
        } else {
            $sLog .= 'GetRedcapRepeatingFormsEvents Error:'.$aREDCapRepeatingFormEvents['error']."\n";
        }

        // fetch REDCap data
        $aREDCapData = hih::GetRedcapData($sAPIToken,array(),array('fields' => $aREDCapFields));

        $aISH_IDs = $aCase_IDs = $aLabParams = array();
        
        // loop over REDcap data
        foreach($aREDCapData as $aData) {

            // mapping $aISH_IDs[record_id] => [ish_id]
            if (isset($aProjConfig['ish_id']) && strlen($aProjConfig['ish_id']) > 0) {
                if (strlen($aData[$aProjConfig['ish_id']]) > 0 && !isset($aISH_IDs[$aData[$sPKREDCap]])) {
                    $aISH_IDs[$aData[$sPKREDCap]] = trim(ltrim($aData[$aProjConfig['ish_id']],'0'));
                }
            }

            // Existing values for repeating instruments
            if (isset($aData['redcap_repeat_instrument']) && strlen($aData['redcap_repeat_instrument']) > 0) {
            
                $sProbeDatum = '';
                if (count($aDateFields) > 0) {
                    foreach($aDateFields as $sDateField => $foo) {
                        $sDateForm = $aREDCapMeta[$sDateField]['form_name'];
                        if ($sDateForm == $aData['redcap_repeat_instrument']) {

                            if (strlen($aData[$sDateField]) > 0) {
                                if (isset($aDateMapping[$aREDCapMeta[$sDateField]['text_validation_type_or_show_slider_number']])) {
                                    $lab_date_format = $aDateMapping[$aREDCapMeta[$sDateField]['text_validation_type_or_show_slider_number']];
                                } else {
                                    $lab_date_format = 'Y-m-d H:i:s';
                                }
                                $date = date_create($aData[$sDateField]);
                                if (is_object($date)) {
                                    $sProbeDatum = date_format($date, $lab_date_format);
                                }
                            }

                            // store existing values in $aLabParams
                            $sEvent = '';
                            if (isset($aData['redcap_event_name'])) {
                                $sEvent = $aData['redcap_event_name'];
                            }

                            $aLabParams[$aData[$sPKREDCap]][$sEvent][$aData['redcap_repeat_instrument']][$sProbeDatum] = $aData;
                        }
                    }
                }
            
            }
        }

        // default tolerance: 0 days
        if (!isset($aProjConfig['tolerance'])) {
            $aProjConfig['tolerance'] = '0';
        }

        $aImport = $aOut = $aHeader = array();

        // loop over REDCap data
        foreach($aREDCapData as $aData) {
        
            // case_id or ish_id empty => skip
            if (strlen($aData[$aProjConfig['case_id']]) == 0) continue;
            
            if (!isset($aISH_IDs[$aData[$sPKREDCap]])) { 
                //$sLog .= "ERROR: No ISH-ID found in record ".$aData[$sPKREDCap]."\n";
                continue;
            }

            $sEvent = '';
            if (isset($aData['redcap_event_name'])) {
                $sEvent = $aData['redcap_event_name'];
            }

            // explode cases by "," (multiple cases per record)
            $aCaseTmp =explode(",",$aData[$aProjConfig['case_id']]);

            // loop over cases and fetch LabResults
            $aImp = $aLabResults2 = array();
            foreach($aCaseTmp as $CaseTmp) {
                
                $iFallNr = trim(ltrim($CaseTmp,'0'));
                // convert case_ids with 9 digits
                if (strlen($iFallNr) == 9) {
                    $iFallNr = substr($iFallNr, 0, -1);
                }
                
                $iISH_ID = $aISH_IDs[$aData[$sPKREDCap]];
                // convert ish_ids with 8 digits
                if (strlen($iISH_ID) == 8) {
                    $iISH_ID = substr($iISH_ID, 0, -1);
                }

                // merge LabResults of multiple cases
                if (isset($aLabResults[$iISH_ID][$iFallNr])) {
                    $aLabResults2 = array_replace_recursive($aLabResults2,$aLabResults[$iISH_ID][$iFallNr]);
                }
            } // end for each cases

            // skip if LabResults are empty
            if (count($aLabResults2) == 0) continue;

            foreach($aProjConfig['labcodes'] as $aLab) {
                foreach($aLab as $sKey => $aVal) {

                    // skip if labcode or redcap_field is empty
                    if (strlen($sKey) == 0 || strlen($aVal['redcap_field']) == 0) continue;
                    
                    // skip field if it doesn't exist in project
                    if (!isset($aREDCapMeta[$aVal['redcap_field']])) continue;
                    
                    // skip field when it's not assigned to REDCap event
                    if (isset($aData['redcap_event_name']) && strlen($aData['redcap_event_name']) > 0) {
                        if (!in_array($aData['redcap_event_name'], $aFormEvents[$aREDCapMeta[$aVal['redcap_field']]['form_name']], true)) {
                            continue;
                        }
                    }
                    // Instrument Name
                    $sFormName = $aREDCapMeta[$aVal['redcap_field']]['form_name'];
                    
                    // import multiple values?
                    if ($aVal['select'] == '5' || $aVal['select'] == '6') {
                        // date field must be set and instrument must be repeating
                        if (strlen($aVal['redcap_lab_date']) == 0 || !isset($aRepeatingFormEvents[$sEvent][$sFormName]))  continue;
                        // date field has to be on the same instrument
                        if ($aREDCapMeta[$aVal['redcap_lab_date']]['form_name'] != $sFormName) continue;
                        // date field has to be in datetime format
                        if (substr($aREDCapMeta[$aVal['redcap_lab_date']]['text_validation_type_or_show_slider_number'],0,8) != 'datetime') continue;
                    }
                    
                    // Results with alternative Labcodes: take the first one with data
                    $aResults = array();
                    $aLabCodes = explode("|",$sKey);
                    foreach($aLabCodes as $sLabCode) {
                        if (isset($aLabResults2[trim($sLabCode)])) {
                            $aResults = $aLabResults2[trim($sLabCode)];
                            break;
                        }
                    }

                    // skip if there are no results for LabCode
                    if (count($aResults) == 0) continue;

                    /************************************************
                    Lab results are available for Case / Labcode      
                    /************************************************/

                    // skip field when event does not match (per field)
                    if (isset($aData['redcap_event_name']) && strlen($aVal['redcap_event']) > 0 && isset($aEvents[$aVal['redcap_event']])) {
                        if ($aEvents[$aVal['redcap_event']] != $aData['redcap_event_name']) {
                            continue;
                        }
                    } 

                    // skip results with data in REDCap (when "full_import" is not checked)
                    if (!isset($aProjConfig['full_import']) || $aProjConfig['full_import'] == 'false') {
                        if (strlen($aData[$aVal['redcap_field']]) > 0) continue;
                    }
                
                    // match result date with REDCap visit date
                    // skip when visit date is empty
                    if (strlen($aVal['redcap_visit_date']) > 0) {
                        if (strlen($aData[$aVal['redcap_visit_date']]) > 0) {
                            // filter results  
                            foreach($aResults as $sDatum => $foo) {
                                if (hih::dateDifference($aData[$aVal['redcap_visit_date']] , $sDatum) > intval($aProjConfig['tolerance'])) {
                                    unset($aResults[$sDatum]);
                                }
                            }                    
                        } else {
                            continue;
                        }
                    } 
                
                    // select result if there is more than one
                    if (count($aResults) > 0) {
                        $aTmp = $aResultNew = array();

                        switch($aVal['select']) {
                  
                            // Kleinster numerischer Wert
                            case '0':
                                foreach($aResults as $sDatum => $aRes) {
                                    $aTmp[$sDatum] = $aRes['result'];
                                }     
                                asort($aTmp, SORT_NUMERIC);  
                                $aResultNew[key($aTmp)] = $aResults[key($aTmp)];
                                $aResults = $aResultNew;
                                break;
                  
                            // Größter numerischer Wert
                            case '1':
                                foreach($aResults as $sDatum => $aRes) {
                                    $aTmp[$sDatum] = $aRes['result'];
                                }     
                                arsort($aTmp, SORT_NUMERIC);  
                                $aResultNew[key($aTmp)] = $aResults[key($aTmp)];
                                $aResults = $aResultNew;
                                break;
    
                            // Frühester Wert
                            case '2':
                                ksort($aResults);  
                                $aResultNew[key($aResults)] = $aResults[key($aResults)];
                                $aResults = $aResultNew;
                                break;
    
                            // Letzter Wert
                            case '3':
                                krsort($aResults);  
                                $aResultNew[key($aResults)] = $aResults[key($aResults)];
                                $aResults = $aResultNew;
                                break;
    
                            // Nächster Wert
                            case '4':
                                if (strlen($aVal['redcap_visit_date']) > 0) {
                                    foreach($aResults as $sDatum => $foo) {
                                        $aTmp[$sDatum] = hih::dateDifference($aData[$aVal['redcap_visit_date']] , $sDatum);
                                    }                    
                                    asort($aTmp, SORT_NUMERIC);  
                                    $aResultNew[key($aTmp)] = $aResults[key($aTmp)];
                                    $aResults = $aResultNew;
                                }
                                break;
                        
                            // Multiple Values: no selection
                            case '5':
                            case '6':
                                break;

                            // default: first value
                            default:
                                $aResultNew[key($aResults)] = $aResults[key($aResults)];
                                $aResults = $aResultNew;
                        } 
                        
                    
                    } else {
                        continue;
                    }

                    /************************************************
                    prepare import array $aImp      
                    /************************************************/
                    foreach($aResults as $sDateTmp => $aResultsTmp) {

                        // convert sample date to date format in REDCap
                        $sProbeDatum = '';
                        if (strlen($aVal['redcap_lab_date']) > 0) {
                            if (isset($aDateMapping[$aREDCapMeta[$aVal['redcap_lab_date']]['text_validation_type_or_show_slider_number']])) {
                                $lab_date_format = $aDateMapping[$aREDCapMeta[$aVal['redcap_lab_date']]['text_validation_type_or_show_slider_number']];
                            } else {
                                $lab_date_format = 'Y-m-d H:i:s';
                            }
                            $date = date_create($sDateTmp);
                            if (is_object($date)) {
                                $sProbeDatum = date_format($date, $lab_date_format);
                            }
                        }

                        // skip results with data in REDCap (when "full_import" is not checked)
                        if (isset($aRepeatingFormEvents[$sEvent][$sFormName])) {
                            if (!isset($aProjConfig['full_import']) || $aProjConfig['full_import'] == 'false') {
                                if (isset($aLabParams[$aData[$sPKREDCap]][$sEvent][$sFormName][$sProbeDatum][$aVal['redcap_field']]) && strlen($aLabParams[$aData[$sPKREDCap]][$sEvent][$sFormName][$sProbeDatum][$aVal['redcap_field']]) > 0) continue;
                            }
                        }

                        // Result
                        $fResult = '';
                        if (isset($aVal['textresults']) && $aVal['textresults'] == '1') {
                            if (!isset($aResultsTmp['resultText'])) {
                                continue;
                            }
                            $aImp[$sFormName][$sProbeDatum][$aVal['redcap_field']] = $aResultsTmp['resultText'];
                        } else {
                            if (!isset($aResultsTmp['result'])) {
                                continue;
                            }
                            $fResult = $aResultsTmp['result'];
                            // number_comma_decimal validation
                            if (strpos($aREDCapMeta[$aVal['redcap_field']]['text_validation_type_or_show_slider_number'], "comma") !== false) {
                                $fResult = str_replace(".", ",", $fResult);
                            }
                            $aImp[$sFormName][$sProbeDatum][$aVal['redcap_field']] = $fResult;
                        }

                        // lab date
                        if (strlen($aVal['redcap_lab_date']) > 0) {
                            if (isset($aRepeatingFormEvents[$sEvent][$sFormName])) {
                                if ($sFormName == $aREDCapMeta[$aVal['redcap_lab_date']]['form_name']) {
                                    $aImp[$sFormName][$sProbeDatum][$aVal['redcap_lab_date']] = $sProbeDatum;
                                }
                            } elseif (!isset($aRepeatingFormEvents[$sEvent][$aREDCapMeta[$aVal['redcap_lab_date']]['form_name']])) { 
                                $aImp[$sFormName][$sProbeDatum][$aVal['redcap_lab_date']] = $sProbeDatum;
                            }
                        }
                        // _unit, _range
                        if (strlen($fResult) > 0) {
                            if (isset($aREDCapMeta[$aVal['redcap_field'].'_unit']) && isset($aResultsTmp['unit']) && $aREDCapMeta[$aVal['redcap_field'].'_unit']['field_type'] == 'text') {
                                $aImp[$sFormName][$sProbeDatum][$aVal['redcap_field'].'_unit'] = $aResultsTmp['unit'];
                            }                
                            if (isset($aREDCapMeta[$aVal['redcap_field'].'_range']) && isset($aResultsTmp['range']) && $aREDCapMeta[$aVal['redcap_field'].'_range']['field_type'] == 'text') {
                                $aImp[$sFormName][$sProbeDatum][$aVal['redcap_field'].'_range'] = $aResultsTmp['range'];
                            }                
                        }
                        // ProVal: _loinc
                        if (isset($GLOBALS['CONFIG']['proval_pid']) && $GLOBALS['CONFIG']['proval_pid'] == $iProj) {
                            if (isset($aREDCapMeta[$aVal['redcap_field'].'_loinc']) && isset($aProvalLOINC[$aVal['redcap_field']])) {
                                $aImp[$sFormName][$sProbeDatum][$aVal['redcap_field'].'_loinc'] = $aProvalLOINC[$aVal['redcap_field']];
                            }                
                        }
                    }     
                } // end foreach($aLab)                         
            } // end foreach($aProjConfig['labcodes'])
            
            // nothing to import => skip
            if (count($aImp) == 0) continue;

            /************************************************
            // build final import array
            /************************************************/

            // build import array
            foreach($aImp as $sFormName => $aImp2) {
                // sort by date (ascending)
                ksort($aImp2);
                foreach($aImp2 as $sProbeDatum => $aTmp) {

                    $aTmp[$sPKREDCap] = $aData[$sPKREDCap];

                    // assign event
                    if (isset($aData['redcap_event_name'])) {
                        $aTmp['redcap_event_name'] = $sEvent;
                    }

                    // set state of forms
                    if (isset($aProjConfig['form_state'])) {
                        $aTmp[$sFormName.'_complete'] = intval($aProjConfig['form_state']);
                    }

                    // assign to repeating instance
                    if (isset($aRepeatingFormEvents[$sEvent][$sFormName])) {
                        if (isset($aLabParams[$aData[$sPKREDCap]][$sEvent][$sFormName][$sProbeDatum]['redcap_repeat_instance']) && strlen($aLabParams[$aData[$sPKREDCap]][$sEvent][$sFormName][$sProbeDatum]['redcap_repeat_instance']) > 0) {
                            $iInstance = $aLabParams[$aData[$sPKREDCap]][$sEvent][$sFormName][$sProbeDatum]['redcap_repeat_instance'];
                        } else {
                            $iInstance = 'new';
                        }
                        $aTmp['redcap_repeat_instrument'] = $sFormName;
                        $aTmp['redcap_repeat_instance'] = $iInstance;

                        // create one dataset for each lab date
                        $aOut[$aData[$sPKREDCap]][$sEvent][$aTmp['redcap_repeat_instrument']][] = $aTmp;
                    } else {
                        $iRep = '';
                        if (isset($aRepeatingFormEvents[$sEvent][''])) {
                            $aTmp['redcap_repeat_instrument'] = '';
                            $iRep = $aTmp['redcap_repeat_instance'] = $aData['redcap_repeat_instance'];
                            
                        }
                        // create one dataset for each event and merge multiple lab entries
                        if (isset($aOut[$aData[$sPKREDCap]][$sEvent][''][$iRep])) {
                            $aOut[$aData[$sPKREDCap]][$sEvent][''][$iRep] = array_merge($aOut[$aData[$sPKREDCap]][$sEvent][''][$iRep],$aTmp);
                        } else {
                            $aOut[$aData[$sPKREDCap]][$sEvent][''][$iRep] = $aTmp;
                        }
                    }

                }
            }    
        
        } // end foreach $aREDCapData

        // build final import array
        foreach($aOut as $iPatID => $aEventOrVisitDates) {
            foreach($aEventOrVisitDates as $sVisitDate => $aRepeatForms) {
                foreach($aRepeatForms as $sRepeatForm => $aTmp) {
                    foreach($aTmp as $iRepeatInstance => $aTmp2) {
                        $aImport[] = $aTmp2;
                    }
                }
            }
        }

        /************************************************
        // do the import
        /************************************************/
        if (count($aImport) > 0) {
            $sLog .= print_r($aImport,true);

            if (!isset($aProjConfig['simulate']) || $aProjConfig['simulate'] == 'false') {
                $result = hih::SaveRedcapData($sAPIToken, $aImport);
                if (!is_int($result)) {
                    $sLog .= "ERROR: ".$result."\n";
                } else {
                    $sLog .= $result." Datensätze aktualisiert!\n";
                }
                if (strlen($sLog) > 0) {
                    $script_end = microtime(true);
                    $time = $script_end - $script_start;    
                    fwrite($handle, "\n\n".date("Y-m-d H:i:s")."\n");
                    fwrite($handle, "Dauer: ".round($time)."s\n");
                    fwrite($handle, $sLog);
                }
            } else {
                foreach($aImport as $row) {
                    foreach($row as $field => $foo) {
                        $aHeader[$field] = true;
                    }
                }
                
                $aHeader2 = array_keys($aHeader);
                if (array_search('redcap_repeat_instance',$aHeader2) !== false) {
                    unset($aHeader2[array_search('redcap_repeat_instance',$aHeader2)]);
                    array_unshift($aHeader2, 'redcap_repeat_instance');
                }
                if (array_search('redcap_repeat_instrument',$aHeader2) !== false) {
                    unset($aHeader2[array_search('redcap_repeat_instrument',$aHeader2)]);
                    array_unshift($aHeader2, 'redcap_repeat_instrument');
                }
                if (array_search('redcap_event_name',$aHeader2) !== false) {
                    unset($aHeader2[array_search('redcap_event_name',$aHeader2)]);
                    array_unshift($aHeader2, 'redcap_event_name');
                }
                unset($aHeader2[array_search($sPKREDCap,$aHeader2)]);
                array_unshift($aHeader2, $sPKREDCap);
                
                $handle_sim = fopen('/tmp/swisslab_import_pid'.$iProj.'_simulate.csv', "w");
                fputcsv($handle_sim, $aHeader2, ";");
                foreach($aImport as $row) {
                    $Vals = array();
                    foreach($aHeader2 as $field) {
                      if (!isset($row[$field])) {
                          $Vals[$field] = '';    
                      } else {
                          $Vals[$field] = '="'.$row[$field].'"';
                      }
                    } 
                    fputcsv($handle_sim,$Vals, ";");
                }
                fclose($handle_sim);
                hih::ImportFileRepository($sAPIToken, '/tmp/swisslab_import_pid'.$iProj.'_simulate.csv', 'application/csv');
            }
        }
        fclose($handle);

    } // end foreach($aConfig
} // end if is_array($aConfig)

?>