<?php

define('WORK_DIR',dirname(__FILE__));
require_once (dirname(__FILE__)."/../../init.php");

//======================================================================================================================================
// Import Lab Results
$aFiles = array_slice(scandir(DATA_DIR), 2);
$aLabResults = array();

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
                
                unlink(DATA_DIR."/".$file);
                
                $iISH_ID = trim(ltrim($aDataBlock['metadata']['patient']['identifier'][0]['value'],'0'));
                $iFallnr = trim(ltrim($aDataBlock['metadata']['additionalResources']['entry'][0]['resource']['identifier'][0]['value'],'0'));
                                   
                // Fallnr prüfen => leer?
                if (strlen($iFallnr) == 0 || strlen($iISH_ID) == 0) {
                    continue;
                }

                if (is_array($aDataBlock['payload'])) {

                    // Datum: Abnahmedatum oder Auftragsdatum
                    if (!isset($aDataBlock['payload']['abnahmeDatum']) || strlen($aDataBlock['payload']['abnahmeDatum']) == 0) {
                        $sDatum = $aDataBlock['payload']['auftragDatum'];
                    } else {
                        $sDatum = $aDataBlock['payload']['abnahmeDatum'];
                    }
                                        
                    foreach($aDataBlock['payload']['resultate'] as $aResults) {

                         if (!isset($aResults['ergebnisZahl'])) {
                            continue;
                         }
                         
                        // wenn Ergebnis leer => weiteres Resultat
                        if (trim($aResults['ergebnisZahl'] == '')) {
                            if (isset($aResults['weiteresResultat']) && is_array($aResults['weiteresResultat'])) {
                                $aResults = $aResults['weiteresResultat'];
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

// get LOINC mappings for ProVal
$aProvalLOINC_csv = hih::csv_to_array("proval_loinc.csv",";");
$aProvalLOINC = array();
foreach($aProvalLOINC_csv as $aTmp) {
    $aProvalLOINC[$aTmp['redcap_field']] = $aTmp['LOINC_Code'];
}

if (is_array($aConfig)) {
    foreach($aConfig as $iProj => $aProjConfig) {
        $script_start = microtime(true);

        // skip if API token does not exist
        if (!isset($GLOBALS['CONFIG']['swisslab_token_'.$iProj])) {
            continue;
        }
        $sAPIToken = $GLOBALS['CONFIG']['swisslab_token_'.$iProj];
        
        // get REDCap fields from config
        $aREDCapFieldsK = array();
        if (isset($aProjConfig['ish_id']) && strlen($aProjConfig['ish_id']) > 0) {
            $aREDCapFieldsK[$aProjConfig['ish_id']] = true;
        }
        if (isset($aProjConfig['case_id']) && strlen($aProjConfig['case_id']) > 0) {
            $aREDCapFieldsK[$aProjConfig['case_id']] = true;
        }
        if (isset($aProjConfig['redcap_instance_lab_date']) && strlen($aProjConfig['redcap_instance_lab_date']) > 0) {
            $aREDCapFieldsK[$aProjConfig['redcap_instance_lab_date']] = true;
        }
        foreach($aProjConfig['labcodes'] as $aLab) {
            foreach($aLab as $sKey => $aVal) {
                $aREDCapFieldsK[$aVal['redcap_field']] = true;
                if (strlen($aVal['redcap_lab_date']) > 0) {
                    $aREDCapFieldsK[$aVal['redcap_lab_date']] = true;
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
        }
        
        // get REDCap forms/events mapping
        $aREDCapFormEvents = hih::GetRedcapFormsEvents($sAPIToken);
        $aFormEvents = array();
        if (!isset($aREDCapFormEvents['error'])) {
            foreach($aREDCapFormEvents as $aEventTmp) {
                $aFormEvents[$aEventTmp['form']][] = $aEventTmp['unique_event_name'];
            }
        }

        // fetch REDCap data
        $aREDCapData = hih::GetRedcapData($sAPIToken,array(),array('fields' => $aREDCapFields));
        
        // project logging
        if (!isset($aProjConfig['full_import']) || $aProjConfig['full_import'] == 'false') {        
            $handle = fopen(dirname(__FILE__).'/logs/swisslab_import_pid'.$iProj.'.txt', "a");
        } else {
            $handle = fopen(dirname(__FILE__).'/logs/swisslab_import_pid'.$iProj.'.txt', "w");
        }
        $sLog = '';

        // loop over REDcap data
        $aISH_IDs = $aCase_IDs = $aLabParams = array();
        foreach($aREDCapData as $aData) {

            // $aISH_IDs[record_id] => [ish_id]
            if (isset($aProjConfig['ish_id']) && strlen($aProjConfig['ish_id']) > 0) {
                if (strlen($aData[$aProjConfig['ish_id']]) > 0 && !isset($aISH_IDs[$aData[$sPKREDCap]])) {
                    $aISH_IDs[$aData[$sPKREDCap]] = trim(ltrim($aData[$aProjConfig['ish_id']],'0'));
                }
            }
            
            if (isset($aProjConfig['all_instances']) && $aProjConfig['all_instances'] == 'true') {
                // $aCase_IDs[ish_id][case_id]['record'] => [record_id]
                // $aCase_IDs[ish_id][case_id]['event'] => [event]
                if (isset($aProjConfig['case_id']) && strlen($aProjConfig['case_id']) > 0) {
                    if (strlen($aData[$aProjConfig['case_id']]) > 0 && isset($aISH_IDs[$aData[$sPKREDCap]])) {
                        $aCaseTmp =explode(",",$aData[$aProjConfig['case_id']]);
                        foreach($aCaseTmp as $CaseTmp) {
                            $aCase_IDs[$aISH_IDs[$aData[$sPKREDCap]]][trim(ltrim($CaseTmp,'0'))]['record'] = $aData[$sPKREDCap];
                            // error if multiple events for case_id
                            if (isset($aCase_IDs[$aISH_IDs[$aData[$sPKREDCap]]][trim(ltrim($CaseTmp,'0'))]['event']) && $aCase_IDs[$aISH_IDs[$aData[$sPKREDCap]]][trim(ltrim($CaseTmp,'0'))]['event'] != $aData['redcap_event_name']) {
                                $sLog .= 'Multiple Events for ISH-ID "'.$aISH_IDs[$aData[$sPKREDCap]].'" and Case "'.$CaseTmp.'"!'."\n";
                                continue;
                            }
                            $aCase_IDs[$aISH_IDs[$aData[$sPKREDCap]]][trim(ltrim($CaseTmp,'0'))]['event'] = $aData['redcap_event_name'];
                        }
                    }
                }
    
                // get existing lab params for import of all instances
                // $aLabParams[record_id][date] => lab data
                if (isset($aProjConfig['redcap_instance_lab_date']) && strlen($aProjConfig['redcap_instance_lab_date']) > 0) {
                    if (strlen($aData['redcap_repeat_instance']) > 0 && strlen($aData[$aProjConfig['redcap_instance_lab_date']]) > 0) {
                        $date = date_create($aData[$aProjConfig['redcap_instance_lab_date']]);
                        if (is_object($date)) {
                            $sProbeDatum = date_format($date, 'Y-m-d H:i:s');
                            $aLabParams[$aData[$sPKREDCap]][$sProbeDatum] = $aData;
                        }
                    }
                }
            }
        }

        // default tolerance: 0 days
        if (!isset($aProjConfig['tolerance'])) {
            $aProjConfig['tolerance'] = '0';
        }

        $aImport = array();

        // import all instances or matched to case / date?
        if (!isset($aProjConfig['all_instances']) || $aProjConfig['all_instances'] == 'false') {
        
            foreach($aREDCapData as $aData) {
            
                // case_id or ish_id empty => skip
                if (strlen($aData[$aProjConfig['case_id']]) == 0) continue;
                
                if (!isset($aISH_IDs[$aData[$sPKREDCap]])) { 
                    $sLog .= "ERROR: No ISH-ID found in record ".$aData[$sPKREDCap]."\n";
                    continue;
                }
                
                $iFallNr = trim(ltrim($aData[$aProjConfig['case_id']],'0'));
                $iISH_ID = $aISH_IDs[$aData[$sPKREDCap]];
    
                $aImp = $aFormComplete = array();
                foreach($aProjConfig['labcodes'] as $aLab) {
                    foreach($aLab as $sKey => $aVal) {
    
                        // skip field if it doesn't exist in project
                        if (!isset($aREDCapMeta[$aVal['redcap_field']])) continue;
                        
                        // skip field when it's not assigned to REDCap event
                        if (isset($aData['redcap_event_name']) && strlen($aData['redcap_event_name']) > 0) {
                            if (!in_array($aData['redcap_event_name'], $aFormEvents[$aREDCapMeta[$aVal['redcap_field']]['form_name']], true)) {
                                continue;
                            }
                        }
        
                        // skip field when event does not match
                        if (isset($aData['redcap_event_name']) && strlen($aVal['redcap_event']) > 0 && isset($aEvents[$aVal['redcap_event']])) {
                            if ($aEvents[$aVal['redcap_event']] != $aData['redcap_event_name']) {
                                continue;
                            }
                        } 
        
                        // skip results with data in REDCap (when "full_import" is not checked)
                        if (!isset($aProjConfig['full_import']) || $aProjConfig['full_import'] == 'false') {
                            if (strlen($aData[$aVal['redcap_field']]) > 0) continue;
                        }
                        
                        // Results with alternative Labcodes
                        $aResults = array();
                        $aLabCodes = explode("|",$sKey);
                        foreach($aLabCodes as $sLabCode) {
                            if (isset($aLabResults[$iISH_ID][$iFallNr][trim($sLabCode)])) {
                                $aResults = $aLabResults[$iISH_ID][$iFallNr][trim($sLabCode)];
                                break;
                            }
                        }
                        // skip if there are no results 
                        if (count($aResults) == 0) continue;
        
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
                        if (count($aResults) > 1) {
                            $aTmp = $aResultNew = array();
    
                            switch($aVal['select']) {
                      
                                // Kleinster numerischer Wert
                                case '0':
                                    foreach($aResults as $sDatum => $aRes) {
                                        $aTmp[$sDatum] = $aRes['result'];
                                    }     
                                    asort($aTmp, SORT_NUMERIC);  
                                    $aResultNew[key($aTmp)] = $aResults[key($aTmp)];
                                    break;
                      
                                // Größter numerischer Wert
                                case '1':
                                    foreach($aResults as $sDatum => $aRes) {
                                        $aTmp[$sDatum] = $aRes['result'];
                                    }     
                                    arsort($aTmp, SORT_NUMERIC);  
                                    $aResultNew[key($aTmp)] = $aResults[key($aTmp)];
                                    break;
        
                                // Frühester Wert
                                case '2':
                                    ksort($aResults);  
                                    $aResultNew[key($aResults)] = $aResults[key($aResults)];
                                    break;
        
                                // Letzter Wert
                                case '3':
                                    krsort($aResults);  
                                    $aResultNew[key($aResults)] = $aResults[key($aResults)];
                                    break;
        
                                // Nächster Wert
                                case '4':
                                    foreach($aResults as $sDatum => $foo) {
                                        $aTmp[$sDatum] = hih::dateDifference($aData[$aVal['redcap_visit_date']] , $sDatum);
                                    }                    
                                    asort($aTmp, SORT_NUMERIC);  
                                    $aResultNew[key($aTmp)] = $aResults[key($aTmp)];
                                    break;
                            
                                // default: first value
                                default:
                                    $aResultNew[key($aResults)] = $aResults[key($aResults)];
                            } 
                            $aResults = $aResultNew;
                        
                        } elseif (count($aResults) == 0) {
                            continue;
                        }
    
                        /************************************************
                        import          
                        /************************************************/
                        // Result
                        $fResult = $aResults[key($aResults)]['result'];
                        // number_comma_decimal validation
                        if (strpos($aREDCapMeta[$aVal['redcap_field']]['text_validation_type_or_show_slider_number'],"comma") !== false) {
                            $fResult = str_replace(".",",",$fResult);
                        }
                        $aImp[$aVal['redcap_field']] = $fResult;
                        
                        // lab date
                        if (strlen($aVal['redcap_lab_date']) > 0) {
                            $date = date_create(key($aResults));
                            $aImp[$aVal['redcap_lab_date']] = date_format($date, 'Y-m-d H:i:s');
                        }
                        
                        // ProVal: _unit, _range, _loinc
                        if ($GLOBALS['CONFIG']['proval_pid'] == $iProj && strlen($fResult) > 0) {
                            if (isset($aREDCapMeta[$aVal['redcap_field'].'_unit']) && isset($aResults[key($aResults)]['unit'])) {
                                $aImp[$aVal['redcap_field'].'_unit'] = $aResults[key($aResults)]['unit'];
                            }                
                            if (isset($aREDCapMeta[$aVal['redcap_field'].'_range']) && isset($aResults[key($aResults)]['range'])) {
                                $aImp[$aVal['redcap_field'].'_range'] = $aResults[key($aResults)]['range'];
                            }                
                            if (isset($aREDCapMeta[$aVal['redcap_field'].'_loinc']) && isset($aProvalLOINC[$aVal['redcap_field']])) {
                                $aImp[$aVal['redcap_field'].'_loinc'] = $aProvalLOINC[$aVal['redcap_field']];
                            }                
                        }
                        
                        // forms to set status
                        $aFormComplete[$aREDCapMeta[$aVal['redcap_field']]['form_name']] = true;
                        
                    } // end foreach($aLab                         
                } // end foreach($aProjConfig['labcodes']
    
                // nothing to import => skip
                if (count($aImp) == 0) continue;
                            
                // build import array
                $aImp[$sPKREDCap] = $aData[$sPKREDCap];
                
                if (isset($aData['redcap_event_name'])) {
                    $aImp['redcap_event_name'] = $aData['redcap_event_name'];
                }
                
                // delete empty repeating instances
                if (strlen($aData['redcap_repeat_instrument']) > 0 && strlen($aData['redcap_repeat_instance']) > 0) {
                    $aImp['redcap_repeat_instrument'] = $aData['redcap_repeat_instrument'];
                    $aImp['redcap_repeat_instance'] = $aData['redcap_repeat_instance'];
                }
                
                // set state of forms if configured
                if (count($aFormComplete) > 0 && isset($aProjConfig['form_state'])) {
                    foreach($aFormComplete as $sForm => $foo) {
                        $aImp[$sForm.'_complete'] = intval($aProjConfig['form_state']);
                    }
                }
    
                $aImport[] = $aImp;
                       
            } // end foreach $aREDCapData
        } // end if !all_instances
        else {
            
            // import all instances => loop over cases in REDCap
            foreach($aCase_IDs as $iISH_ID => $aCasesTmp) {
                foreach($aCasesTmp as $iFallNr => $aCase) {
                    
                    // no results -> skip
                    if (!isset($aLabResults[$iISH_ID][$iFallNr])) continue;

                    $RecordID = $aCase['record'];
                    $Event = $aCase['event'];
                    
                    // skip field when event does not match
                    if (isset($aProjConfig['redcap_instance_event']) && isset($aEvents[$aProjConfig['redcap_instance_event']])) {
                        if ($aEvents[$aProjConfig['redcap_instance_event']] != $Event) {
                            continue;
                        }
                    } 

                    $aImp = array();
                    foreach($aProjConfig['labcodes'] as $aLab) {
                        foreach($aLab as $sKey => $aVal) {
        
                            // skip field if it doesn't exist in project
                            if (!isset($aREDCapMeta[$aVal['redcap_field']])) continue;
                            
                            // skip field when it's not assigned to REDCap event
                            if (strlen($Event) > 0 && !in_array($Event, $aFormEvents[$aREDCapMeta[$aVal['redcap_field']]['form_name']], true)) {
                                continue;
                            }

                            // Results with alternative Labcodes
                            $aResults = array();
                            $aLabCodes = explode("|",$sKey);
                            foreach($aLabCodes as $sLabCode) {
                                if (isset($aLabResults[$iISH_ID][$iFallNr][trim($sLabCode)])) {
                                    $aResults = $aLabResults[$iISH_ID][$iFallNr][trim($sLabCode)];
                                    break;
                                }
                            }
                            // skip if there are no results 
                            if (count($aResults) == 0) continue;
            
                            /************************************************
                            import          
                            /************************************************/
                            foreach($aResults as $sDateTmp => $aResultsTmp) {
                                $date = date_create($sDateTmp);
                                $date_db = date_format($date, 'Y-m-d H:i:s');
    
                                // skip results with data in REDCap (when "full_import" is not checked)
                                if (!isset($aProjConfig['full_import']) || $aProjConfig['full_import'] == 'false') {
                                    if (strlen($aLabParams[$RecordID][$date_db][$aVal['redcap_field']]) > 0) continue;
                                }
                    
                                // Result
                                $fResult = $aResultsTmp['result'];
                                // number_comma_decimal validation
                                if (strpos($aREDCapMeta[$aVal['redcap_field']]['text_validation_type_or_show_slider_number'],"comma") !== false) {
                                    $fResult = str_replace(".",",",$fResult);
                                }
                                $aImp[$date_db][$aVal['redcap_field']] = $fResult;
                            
                                if (strlen($fResult) > 0) {
                                    if (isset($aREDCapMeta[$aVal['redcap_field'].'_unit']) && isset($aResultsTmp['unit']) && $aREDCapMeta[$aVal['redcap_field'].'_unit']['field_type'] == 'text') {
                                        $aImp[$date_db][$aVal['redcap_field'].'_unit'] = $aResultsTmp['unit'];
                                    }                
                                    if (isset($aREDCapMeta[$aVal['redcap_field'].'_range']) && isset($aResultsTmp['range']) && $aREDCapMeta[$aVal['redcap_field'].'_range']['field_type'] == 'text') {
                                        $aImp[$date_db][$aVal['redcap_field'].'_range'] = $aResultsTmp['range'];
                                    }                
                                }
                            }                            
                            
                        } // end foreach($aLab                         
                    } // end foreach($aProjConfig['labcodes']

                    // nothing to import => skip
                    if (count($aImp) == 0) continue;

                    // sort by date (ascending)
                    ksort($aImp);
                    
                    // build import array
                    foreach($aImp as $sProbeDatum => $aTmp) {
                        // Probedatum exists already
                        if (isset($aLabParams[$RecordID][$sProbeDatum]['redcap_repeat_instance']) && strlen($aLabParams[$RecordID][$sProbeDatum]['redcap_repeat_instance']) > 0) {
                            $iInstance = $aLabParams[$RecordID][$sProbeDatum]['redcap_repeat_instance'];
                        } else {
                            $iInstance = 'new';
                        }
                        
                        $aTmp['record_id'] = $RecordID;
                        $aTmp['redcap_event_name'] = $Event;
                        $aTmp['redcap_repeat_instrument'] = $aREDCapMeta[$aProjConfig['redcap_instance_lab_date']]['form_name'];
                        $aTmp['redcap_repeat_instance'] = $iInstance;
                        $aTmp[$aProjConfig['redcap_instance_lab_date']] = $sProbeDatum;

                        // set state of forms if configured
                        if (isset($aProjConfig['form_state'])) {
                            $aTmp[$aREDCapMeta[$aProjConfig['redcap_instance_lab_date']]['form_name'].'_complete'] = intval($aProjConfig['form_state']);
                        }

                        $aImport[] = $aTmp;
                    }
                    
                } // end foreach($aCasesTmp
            } // end foreach($aCase_IDs        
        } // end if all_instances

        if (count($aImport) > 0) {
            $sLog .= print_r($aImport,true);
            $result = hih::SaveRedcapData($sAPIToken, $aImport);
            
            if (!is_int($result)) {
                $sLog .= "ERROR: ".$result."\n";
            } else {
                $sLog .= $result." Datensätze aktualisiert!\n";
            }
        }

        if (strlen($sLog) > 0) {
            $script_end = microtime(true);
            $time = $script_end - $script_start;    
            fwrite($handle, "\n\n".date("Y-m-d H:i:s")."\n");
            fwrite($handle, "Dauer: ".round($time)."s\n");
            fwrite($handle, $sLog);
        }
        
        fclose($handle);

    } // end foreach($aConfig
} // end if is_array($aConfig)

?>