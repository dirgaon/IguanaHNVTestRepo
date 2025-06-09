require('retry')
PACSDBPROD = {}

PACSDBPROD.sSynapseServer = 'SYNAPSE-DB01'
PACSDBPROD.sDbUser = 'iguana'
PACSDBPROD.sDbPassword = 'IguanaUs3r'

function PACSDBPROD.PACSConnection(bLive)
   if not bLive then
      bLive = true
   end
   
   
    local sConnection = db.connect{   
      api=db.ORACLE_ODBC, 
      name=PACSDBPROD.sSynapseServer,
      user=PACSDBPROD.sDbUser,
      password=PACSDBPROD.sDbPassword,
      use_unicode = true     
   }
   
 --  local sConnection = [[(DESCRIPTION = 
   --                 (ADDRESS = (PROTOCOL = TCP)
   --                     (HOST = ]] .. PACSDB.sSynapseServer .. [[)
   --                     (PORT = 1521))
     --                (CONNECT_DATA = (SERVER = DEDICATED)
       --                  (SERVICE_NAME = FOOD.WORLD)))]]
   
   --return retry.call{func=db.connect, arg1= {api=db.ORACLE_OCI, name=sConnection, user=PACSDB.sDbUser, password=PACSDB.sDbPassword, use_unicode=true,live=bLive}}    
   return sConnection
end

--+++++++++++++++++++++++++++++++++++
-- protected query
-- Returns success (boolean) and result (table or error object)
function PACSDBPROD.pquery(dbConn, sSQL)      
   return pcall(dbConn.query, dbConn, {sql = sSQL})
end

--+++++++++++++++++++++++++++++++++++
-- protected execute
-- Returns success (boolean) and result (table or error object)
function PACSDBPROD.pexecute(dbConn, sSQL, bLive)
   if not bLive then
      bLive = false
   end
   return pcall(dbConn.execute, dbConn, {sql = sSQL, live = bLive})
end
