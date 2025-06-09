require "PACS DB"
require "retry"
require "PACS DB"

-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function main(Data)
   --iguana.log("starting")
  

   -- mock sample
   --MSH|^~\&|PROSOLV|PROSOLV|HIS|HIS|202411281931||ORU^R01|2411281931388|P|2.3
   --PID||12345|12345||LASTNAME^Firstname||19320203|F|||||||||||
   --PV1|||||||^Gordon^Dr Stephen^^|Dr E Yamen|||||||||
   --OBR|1||235244|^TTX(Adult)^|||202411281509|||||||||||||||202411281931|||F||||||NULL^^I9M|^Gordon^Dr Stephen^^|||^Gordon^Dr Stephen^^
   --OBX|1|TX|^TTX(Adult)^||~              Echocardiogram Report~~~ Name:    LASTNAME, Firstname ...~||||||F
   --OBX|2|ED|PDF^Display Format in PDF^PLS|^TTX(Adult)^|^PDF^PDF^BASE64^JVBERi0...==||||||F|

   --current

   --MSH|^~\&|ER|WC|HIS|WC|20250325094627||ORU^R01|123420250325094627|P|2.3.1
   --PID||103093|103093||Newton^James^|||M|||||||||||
   --PV1|||||||Peter Vial|
   --OBR|1||1000001|^WC DSE Report^|||20240905|||||||||||||||20250325094619|||P|||||||PVIAL^Peter Vial|||Peter Vial
   --OBX|1|ED|PDF^Display Formate in PDF^PLS|^WC DSE Report^|^PDF^PDF^BASE64^JVBERi0...==||||||P

   msgin, sname = hl7.parse{vmd='Outbound CV Report.vmd',data=Data}
   msgout = hl7.message{vmd='Outbound CV Report.vmd',name=sname}
   msgout:mapTree(msgin)
   if sname == 'CVReport' then

      


      --MSH
      --MSH|^~\&|PROSOLV|PROSOLV|HIS|HIS|202411281931||ORU^R01|2411281931388|P|2.3
      --MSH|^~\&|ER|WC|HIS|WC|20250325094627||ORU^R01|123420250325094627|P|2.3.1

      msgout.MSH[3][1] = 'PROSOLV'
      msgout.MSH[4][1] = 'PROSOLV'
      msgout.MSH[12] = '2.3'



      --PID
      --PID||12345|12345||LASTNAME^Firstname||19320203|F|||||||||||
      --PID||103093|103093||Newton^James^|||M|||||||||||

      --no changes so far


      --PV1
      --PV1|||||||^Gordon^Dr Stephen^^|Dr E Yamen|||||||||
      --PV1|||||||Peter Vial|

      msgout.PV1[7]:mapTree(msgin.OBR[32])


      --OBR
      --OBR|1||235244|^TTX(Adult)^|||202411281509|||||||||||||||202411281931|||F||||||NULL^^I9M|^Gordon^Dr Stephen^^|||^Gordon^Dr Stephen^^
      --OBR|1||1000001|^WC DSE Report^|||20240905|||||||||||||||20250325094619|||P|||||||PVIAL^Peter Vial|||Peter Vial

      
      msgout.OBR[3][1] = msgin.OBR[3][1]:S()
      
      msgout.OBR[22] = msgin.OBR[22]:S():sub(1,12)
      msgout.OBR[35]:mapTree(msgin.OBR[32][1])

      --lookup accession number
      
      --test
      

 --     local dbCon = db.connect{   
--      api=db.ORACLE_ODBC, 
 --     name=[[SYNAPSE-TEST01]],
--      user='iguana',
  --    password='IguanaUs3r',
 --     use_unicode = true,
 --     live = true
 --  }
 
  -- conn:query{sql='SELECT * FROM synapse.study'}


      local dbCon = PACSDB.PACSConnection()
     -- iguana.log("got here")


      if dbCon then
         --iguana.log("got here too")



         attcode = msgin.OBR[32][1]:S()

         local sql = [[SELECT ris_study_euid from synapse.study where id = ]] .. msgin.OBR[3][1]:S()
         trace(sql)

         local tQuery = dbCon:query{sql=sql}
         

         trace(tQuery)

         if tQuery[1].RIS_STUDY_EUID:S() == nil or tQuery[1].RIS_STUDY_EUID:S() == [[NULL]] then
      
          --msgout.OBR[3][1] = 'SYN-' .. msgin.OBR[3][1]:S()
         -- instructed to leave empty if no Accession number
            msgout.OBR[2] = nil


            --update database


           -- local accsql = [[update synapse.study set ris_study_euid = 'SYN-]] .. msgin.OBR[3][1]:S() .. [[' where id = ]] .. msgin.OBR[3][1]:S()
           -- trace(accsql)
           -- iguana.log(accsql)

           --local accQuery = dbCon:execute{sql=accsql}
           -- dbCon:commit()
            
         else
            --accession number in OBR[2]
           msgout.OBR[2][1] = tQuery[1].RIS_STUDY_EUID:S()
           msgout.OBR[2][2] = 'HTBOOKINGID'
         end

      end
      dbCon:close()

      --OBX 1
      --OBX|1|TX|^TTX(Adult)^||~              Echocardiogram Report~~~ Name:    LASTNAME, Firstname ...~||||||F
      -- na

      msgout.OBX = nil

     -- msgout.OBX[1][1] = '1'
     -- msgout.OBX[1][2] = 'TX'
     -- msgout.OBX[1][3]:mapTree(msgin.OBR[4])
     -- msgout.OBX[1][11] = msgin.OBR[25]

      --load TEXT file
     -- msgout.OBX[1][5][1][1][1] = msgin.OBX[2][5][1][1][1]:S()


      --OBX 2
      --OBX|2|ED|PDF^Display Format in PDF^PLS|^TTX(Adult)^|^PDF^PDF^BASE64^JVBERi0...==||||||F|
      --OBX|1|ED|PDF^Display Formate in PDF^PLS|^WC DSE Report^|^PDF^PDF^BASE64^JVBERi0...==||||||P

      msgout.OBX[1]:mapTree(msgin.OBX[1])
      msgout.OBX[1][1] = '1'
      msgout.OBX[1][3][2] = 'Display Format in PDF'
      msgout.OBX[1][11] = msgin.OBR[25]

      

      
      --iguana.log("about to do database update")
      -- do database updates
     UpdateDatabase(Data)
      --upload PDF
      uploadPDF(Data)
      
      queue.push{data=msgout:S()}

   end



end







-- The main function is the first function called from Iguana.
-- The Data argument will contain the message to be processed.
function UpdateDatabase(Data)
iguana.log("doing database update")

   msgin = hl7.parse{vmd='Outbound CV Report.vmd',data=Data}

   --get acc and dr
   -- accnum = msgin.OBR[3][1]:S()
   studyuid = msgin.OBR[3][1]:S()

   if studyuid ~= '' then
      trace( msgin.OBR[20]:S())

      local dbCon = PACSDB.PACSConnection()
      docid = 0

      if dbCon then
         iguana.log("got into dbcon")

         --update procedure

         -- local procsql = [[UPDATE synapse.study SET procedure_info_uid = (select ID from synapse.procedure_info where code = ']] .. msgin.OBR[4][2]:S() ..[[') where id = ]] ..msgin.OBR[3][1]:S()
        -- trace (procsql)
       
     --  local procQuery = dbCon:execute{sql=procsql}
     --    dbCon:commit()

         -- update site if containing midland

      --   trace(msgin.PV1[3][1]:S() )

      --   if msgin.PV1[3][1]:S() ~= '' and string.find(string.upper(msgin.PV1[3][1]:S()),[[MIDLAND]]) ~= nil then
     --       local sitesql = [[UPDATE synapse.study SET site_uid = (select ID from synapse.site where EUID = 'MIDLAND') where id = ]] ..msgin.OBR[3][1]:S()
     --       trace (sitesql)
     --       local siteQuery = dbCon:execute{sql=sitesql}

     --    end




         local docfound = false


         --USE OBR21 if available as it's more accurate

         if msgin.OBR[32]:S() ~= nil and msgin.OBR[32]:S() ~= '' then

            attcode = msgin.OBR[32][1]:S()

            local sql = [[SELECT distinct USERINFOUID as ID from synapse.user_login_info_all_view where not ROLENAME like 'SWAT%' and upper(LOGINNAME) like ']] .. string.gsub(attcode,[[']],[['']]) .. [[']]
            trace(sql)

            local tQuery = dbCon:query{sql=sql}

            trace(tQuery)




            if #tQuery == 1 then
               docfound = true
               docid = tonumber(tQuery[1].ID:S())

            elseif #tQuery > 1 then

               iguana.logError("Multiple users found: " .. attcode .. " for studyuid: " .. studyuid .. [[, using the first one found: ]] .. tonumber(tQuery[1].ID:S())  )
               --pick one at random, what can you do


               docfound = true
               docid = tonumber(tQuery[1].ID:S())


            else

               iguana.log("User not found: " .. attcode .. " for studyuid: " .. studyuid)
            end




         end -- endif obr21








         --  if docfound == false and msgin.OBR[20]:S() ~= nil and msgin.OBR[20]:S() ~= '' then


         --    attphy = string.upper(msgin.OBR[20]:S()):gsub([[\T\#39;]],[[']])


         -------------------------------------------------------------------------------------
         --CHANGE METHOD, firstname last name should match ompletely as is
         -------------------------------------------------------------------------------------


         --fimd physician:
         --example
         --firstname || ' ' || lastname like '  Dr Raj   Kanna Mbbs Mrcp (Uk) Fracp'

         --local sql = [[select id from synapse.user_info where upper(user_euid) = ']] .. string.gsub(attphy,[[']],[['']]) .. [[' or  upper(user_euid) = ']] .. string.gsub(attphynotitle,[[']],[['']]) .. [[']]

         --         local sql = [[SELECT distinct USERINFOUID as ID from synapse.user_login_info_all_view where upper(firstname) || ' ' || upper(lastname) like ']] .. string.gsub(attphy,[[']],[['']]) .. [[']]
         --         trace(sql)

         --          local tQuery = dbCon:query{sql=sql}

         --          trace(tQuery)

         --           local docid = 0


         --            if #tQuery == 1 then
         --              docfound = true
         --             docid = tonumber(tQuery[1].ID:S())

         -- -          elseif #tQuery > 1 then

         --             iguana.logError("Multiple users found: " .. attphy  .. " for studyuid: " .. studyuid .. [[, using the first one found: ]] .. tonumber(tQuery[1].ID:S())  )
         --pick one at random, what can you do


         --              docfound = true
         --              docid = tonumber(tQuery[1].ID:S())


         --         else





         --             iguana.logError("User not found: " .. attphy .. " for studyuid: " .. studyuid)
         --          end

         --       end -- end if obr20





         trace(docfound)
         trace(docid)



         iguana.log("about to go docfound: " .. docid)

         if docfound  then

            iguana.log("inside docfound")

            --if prelim just update attending physician
            if msgin.OBR[25]:S() == 'F' then

               --see if study has visit, if not create one

               local sqlvisit = [[select id from synapse.visit where id in (select visit_uid from synapse.study where not visit_uid = -1 and id = ']] .. studyuid .. [[')]]
               local tQueryvisit = dbCon:query{sql=sqlvisit}

               trace(#tQueryvisit)
               iguana.log('visit found: ' .. tQueryvisit[1].ID:S())
               

               if #tQueryvisit == 0 then
                  --create visit and insert attphy

                  --get next val of synapse visit
                  
                  iguana.log('creating visit')
                  local sqlvisitid = [[select SYNAPSE.VISIT_SEQ.nextval from dual]]
                  local tQueryvisitid = dbCon:query{sql=sqlvisitid}

                  trace(tQueryvisitid)


                  --ALSO UPDATE LOCATION

                  --  locationid = -1
                  -- if msgin.OBR[13]:S() ~= "" then


                  --       local sqlLocation = [[select id from synapse.location where name = ']] .. msgin.OBR[13]:S() .. [[']]
                  --        local tQueryLocation = dbCon:query{sql=sqlLocation}

                  --       trace(tQueryLocation)

                  --        if #tQueryLocation == 0 then

                  --location not found, log error
                  --         iguana.logError("Location not found, please create location with name: " .. msgin.OBR[13]:S())

                  --     elseif  #tQueryLocation > 1 then

                  -- log multiple location same name
                  --          iguana.logError("Multiple location found with name: " .. msgin.OBR[13]:S())

                  --      else 
                  --          iguana.log("Location found: " .. msgin.OBR[13]:S() .. " with id " .. tQueryLocation[1].ID:S())
                  --          locationid = tQueryLocation[1].ID:S()
                  --       end
                  --    end





                  iguana.log('inserting visit')


                  local sqlinsert = [[insert into synapse.visit (id,patient_uid, site_uid, visit_number, attending_physician_uid, primary_location_uid, current_location_uid,class,referring_physician_uid) values ]] ..
                  [[( ]] .. tonumber(tQueryvisitid[1].NEXTVAL:S()) .. [[, ]] ..
                  [[(Select patient_uid from synapse.study where id = ']] .. studyuid .. [['), ]] ..
                  [[(Select site_uid from synapse.study where id = ']] .. studyuid .. [['), ]] ..
                  [[']] .. studyuid .. [[', ]] ..
                  docid .. 
                  [[,-1,-1,'<unknown>',-1)]]

                  trace(sqlinsert)
                  iguana.log(sqlinsert)
                  local Success, Result = pcall(executeAndCommit, dbCon,sqlinsert)   
                  
                  if not Success then   
                     iguana.log("Skipping error: " .. Result[1]:S())   
                     error("Fatal error occurred: ".. Result[1]:S())   
                  end   
                 
                  dbCon:commit()
                  
                  iguana.log('linking visit')

                  local sqlupdate = [[update synapse.study set visit_uid = ]] .. tonumber(tQueryvisitid[1].NEXTVAL:S()) .. [[  where id = ']] .. studyuid .. [[']]
                  trace (sqlupdate)
                  local tQueryupdate = dbCon:execute{sql=sqlupdate}
                  dbCon:commit()

                  iguana.log("Synapse Visit created and updated: " .. tonumber(tQueryvisitid[1].NEXTVAL:S()) .. " studyuid: " .. studyuid .. " updated with attending physician: " .. docid  )


               else
                  --update visit

                  --ALSO UPDATE LOCATION


                  locationid = -1
                  --  if msgin.OBR[13]:S() ~= "" then

                  --        local sqlLocation = [[select id from synapse.location where name = ']] .. msgin.OBR[13]:S() .. [[']]
                  --        local tQueryLocation = dbCon:query{sql=sqlLocation}

                  --        trace(tQueryLocation)

                  --        if #tQueryLocation == 0 then

                  --location not found, log error
                  --            iguana.logError("Location not found, please create location with name: " .. msgin.OBR[13]:S())

                  --        elseif  #tQueryLocation > 1 then

                  -- log multiple location same name
                  --           iguana.logError("Multiple location found with name: " .. msgin.OBR[13]:S())

                  --      else 
                  --          iguana.log("Location found: " .. msgin.OBR[13]:S() .. " with id " .. tQueryLocation[1].ID:S())
                  --           locationid = tQueryLocation[1].ID:S()
                  --        end
                  --     end


                  
                  iguana.log("about to update attending phy: " .. tonumber(tQueryvisit[1].ID:S()) .. " doc id:" .. docid)

                  --local sqlupdate = [[update synapse.visit set attending_physician_uid = ]] .. tonumber(tQueryvisit[1].ID:S()) .. [[, primary_location_uid = ]] .. locationid.. [[ where id = ]] .. tQueryvisit[1].ID:S()
                  local sqlupdate = [[update synapse.visit set attending_physician_uid = ]] .. docid .. [[ where id = ]] .. tQueryvisit[1].ID:S()
                  local tQueryupdate = dbCon:execute{sql=sqlupdate}
                  dbCon:commit()
                  
                  iguana.log("Synapse Visit updated: " .. tQueryvisit[1].ID:S().. " studyuid: " .. studyuid  .. " updated with attending physician: " .. docid)
               end -- end visit query




               -- if FINAL, also update dictated



               local sqlinsert = [[insert into synapse.study_medical_event (id, study_uid,activity_uid,user_info_uid,event_timedate,create_timedate) VALUES ]] ..

               [[(SYNAPSE.study_med_event_SEQ.nextval, ]] .. studyuid ..[[, 7,]] ..
               docid .. [[,sysdate,sysdate)]]
               trace(sqlinsert)
               local tQueryinsert = dbCon:execute{sql=sqlinsert}
               dbCon:commit()




            end -- end iffinal




         end --end docfound 





      else
         iguana.logError("Database issue")
      end

      dbCon:close()

   else

      iguana.logError("No Accession Number")


   end

end

function uploadPDF(Data)

   iguana.log("doing PDF Upload")
   
   --check current report
   local doccount = -1
   local docid = 0
   local storageuid = 0
   local storagepath = ''
   local accexists = 0
   local ReportName = 'PDFReport'
   local extension = '.pdf'

   msgin, sname = hl7.parse{vmd='Outbound CV Report.vmd',data=Data}




   local ReportStatus =  msgin.OBR[25]:S()

   local dbCon = PACSDB.PACSConnection()

   local doccount = 0
   local docid = 0
   local commclass = 0
   local storageuid = 0
   local storagepath = ''
   local accexists = 0

   local studyuid = msgin.OBR[3][1]:S()

   if dbCon then
      local sql = [[SELECT ID ]] .. 
      [[ ,(SELECT count(ID) + 1 FROM SYNAPSE.DOCUMENT doc WHERE NAME like ']]  ..  ReportName  ..  [[' AND IS_OBSOLETE = 'N' 
      AND doc.id in (SELECT DOCUMENT_UID FROM SYNAPSE.STUDY_DOCUMENT stdoc WHERE STUDY_UID = st.id)) as Count]] .. 
      [[ ,SYNAPSE.DOCUMENT_SEQ.nextval ]] .. 
      [[ ,(SELECT ID FROM SYNAPSE.COMMAND_CLASS WHERE NAME = 'RadiologicalReportDocObject') as commclass]] .. 
      [[ ,(SELECT STORAGE_UID FROM SYNAPSE.OBJECT_TYPE WHERE DISPLAY_NAME_US = 'Report') as storageuid ]] .. 
      [[ ,(SELECT UNC_PATH FROM SYNAPSE.STORAGE WHERE ID = (SELECT STORAGE_UID FROM SYNAPSE.OBJECT_TYPE WHERE DISPLAY_NAME_US = 'Report')) as storagepath]] .. 
      [[ FROM SYNAPSE.STUDY st ]] .. 
      [[ WHERE  st.id = ]] .. studyuid  .. [[]]

      trace(sql)
      tQuery = dbCon:query{sql=sql}
      trace(tQuery)

      if #tQuery > 0 then

         studyuid = tonumber(tQuery[1].ID:S())
         doccount = tonumber(tQuery[1].COUNT:S())
         docid = tonumber(tQuery[1].NEXTVAL:S())
         commclass = tonumber(tQuery[1].COMMCLASS:S())
         storageuid =  tonumber(tQuery[1].STORAGEUID:S())
         storagepath = tQuery[1].STORAGEPATH:S()
         accexists = 1

      end -- end if tQuery > 0 



   end --end if dbcon


   if (accexists == 0) then
      iguana.logError("Study not found, studyuid: " .. studyuid)
   else


      if (doccount > 1) then


         sql = [[UPDATE SYNAPSE.DOCUMENT doc SET IS_OBSOLETE = 'Y', DOCUMENT_DELETED_DATE = sysdate ]] .. 
         [[WHERE NAME like ']] .. ReportName .. [[' AND IS_OBSOLETE = 'N' AND doc.id in ]] .. 
         [[(SELECT DOCUMENT_UID FROM SYNAPSE.STUDY_DOCUMENT stdoc WHERE study_uid = ]] .. studyuid .. [[)]]

         local result = dbCon:execute{sql=sql};
         dbCon:commit()

         iguana.log(sql)


         sql = [[UPDATE SYNAPSE.STUDY SET DOCUMENT_COUNT = DOCUMENT_COUNT - 1, STATUS = 50 WHERE DOCUMENT_COUNT > 0 AND ID = ]] .. studyuid

         result = dbCon:execute{sql=sql}
         dbCon:commit()
         iguana.log(sql)



      end -- end if doccount > 1

      currentdate = os.date("%Y%m%d")
      local dir = storagepath .. currentdate .. [[\]] .. studyuid
      local filename = ReportName .. [[-]] .. docid .. extension


      --create document

      --decode pdf.
      trace(msgin.OBX[1][5][1][5]:S())
      pdffile = filter.base64.dec(msgin.OBX[1][5][1][5]:S())






      --20240705\\1000195\\PDFReport-1000146.pdf
      pdflocation = storagepath .. [[PDFReports\]] .. currentdate .. [[-]] .. studyuid .. [[-]] .. filename
      trace(pdflocation)

      file = io.open(pdflocation, "wb")
      file:write(pdffile)
      file:close()




      sql = [[INSERT INTO SYNAPSE.DOCUMENT]] ..
      [[ (ID,COMMAND_CLASS_UID,FILENAME,NAME,ACR_UID,DIAGNOSTIC_CODE_UID, CREATION_TIMEDATE,STORAGE_UID,IS_OBSOLETE,LAST_MODIFICATION_TIMEDATE)]] ..
      [[ VALUES ]] ..
      [[ (]] .. docid .. [[,]] .. commclass .. [[,']] .. [[PDFReports\]] .. currentdate .. [[-]] .. studyuid .. [[-]] .. filename .. [[',']] .. ReportName 
      .. [[',-1,-1,sysdate,]] .. storageuid .. [[,'N',sysdate)]]

      trace(sql)

      result = dbCon:execute{sql=sql}
      dbCon:commit()
      iguana.log(sql)

      --link document

      sql = [[INSERT INTO SYNAPSE.STUDY_DOCUMENT ]] ..
      [[ ( ID, STUDY_UID, DOCUMENT_UID ) ]] ..
      [[ SELECT  SYNAPSE.STUDY_DOCUMENT_SEQ.nextval,]] .. studyuid .. [[,]] .. docid .. [[ FROM Dual]]

      result = dbCon:execute{sql=sql}
      dbCon:commit()
      iguana.log(sql)

      --update count and status
      if msgin.OBR[25]:S() == 'P' then

         sql = [[UPDATE SYNAPSE.STUDY SET DOCUMENT_COUNT = DOCUMENT_COUNT + 1, status = 45, report_status = 10 WHERE ID = ]] .. studyuid



      elseif msgin.OBR[25]:S() == 'F' then
         sql = [[UPDATE SYNAPSE.STUDY SET DOCUMENT_COUNT = DOCUMENT_COUNT + 1, status = 60, report_status = 30 WHERE ID = ]] .. studyuid

      elseif msgin.OBR[25]:S() == 'C' then

         sql = [[UPDATE SYNAPSE.STUDY SET DOCUMENT_COUNT = DOCUMENT_COUNT + 1, status = 65, report_status = 20 WHERE ID = ]] .. studyuid
      end

      result = dbCon:execute{sql=sql}
      dbCon:commit()
      iguana.log(sql)


      dbCon:close()
   end -- end if accexists

end -- end main

function executeAndCommit(dbcon,query)
   dbcon:execute{sql=query}
   dbcon:commit()
end
