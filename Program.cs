using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using SAP.Middleware.Connector;
using System.Data;
using System.Data.SqlClient; 
namespace SincronizacionSapCompras
{
 

    public class ECCDestinationConfig : IDestinationConfiguration
    {

        public bool ChangeEventsSupported()
        {
            return false;
        }

        public event RfcDestinationManager.ConfigurationChangeHandler ConfigurationChanged;
        public DataTable ConvertToDotNetTable(IRfcTable RFCTable)
        {
            DataTable dtTable = new DataTable();
            for (int item = 0; item < RFCTable.ElementCount; item++)
            {
                RfcElementMetadata metadata = RFCTable.GetElementMetadata(item);
                dtTable.Columns.Add(metadata.Name);
            }

            foreach (IRfcStructure row in RFCTable)
            {

                DataRow dr = dtTable.NewRow();
                for (int item = 0; item < RFCTable.ElementCount; item++)
                {
                    RfcElementMetadata metadata = RFCTable.GetElementMetadata(item);
                    if (metadata.DataType == RfcDataType.BCD && metadata.Name == "ABC")
                    {
                        dr[item] = row.GetInt(metadata.Name);
                    }
                    else
                    {
                        dr[item] = row.GetString(metadata.Name);
                    }


                }
                dtTable.Rows.Add(dr);
}

            return dtTable;

        }
        public RfcConfigParameters GetParameters(string destinationName)
        {

            RfcConfigParameters parms = new RfcConfigParameters();


           

            parms.Add(RfcConfigParameters.Name, "TGQ");
            parms.Add(RfcConfigParameters.User, "RFC_USR");
            parms.Add(RfcConfigParameters.Password, "Trcliente*719");
            parms.Add(RfcConfigParameters.Client, "300");
            parms.Add(RfcConfigParameters.AppServerHost, "10.1.2.50");
            parms.Add(RfcConfigParameters.SystemNumber, "50");
            parms.Add(RfcConfigParameters.Language, "ES");
            parms.Add(RfcConfigParameters.PoolSize, "10");
            return parms; 

        }
    
    
    
    }
    
    
    
    
    
    class Program
    {
        public static string cadenaConexionDestino = "Data Source=10.1.2.32" + "\\" + "Calidad;initial catalog=OptimizacionCompras ; User ID=UsrOptimizacionCompras;Password=UsrOptimizacionCompras;Integrated Security=SSPI;"; 

        public static  void cargarProveedores()        
        {
        
            ECCDestinationConfig cfg = new ECCDestinationConfig();
            RfcDestinationManager.RegisterDestinationConfiguration(cfg);
            RfcDestination dest = RfcDestinationManager.GetDestination("TGQ");
            RfcRepository rfcRepository = dest.Repository;

            IRfcFunction rfcfunction = rfcRepository.CreateFunction("ZPROVEDORES");
            rfcfunction.Invoke(dest);
            IRfcTable tblUserInfo = rfcfunction.GetTable(0) ;
            DataTable dtbl = cfg.ConvertToDotNetTable(tblUserInfo);
            List<string> listadoColumnas = new List<string>();
            foreach (DataColumn cl in dtbl.Columns)
            {
                System.Console.WriteLine(cl.ColumnName); 
                listadoColumnas.Add(cl.ColumnName);
            }
            
        foreach (DataRow fila in dtbl.Rows)
        {    
            #region  Insercion a tabla de proveedores 
            SqlConnection cn = new SqlConnection(cadenaConexionDestino);
            String query = @" Merge OptimizacionCompras..Proveedores as target   
                              Using  (Select @descripcion,@nit,@numeroCuenta,@direccion) as source (descripcion, nit ,numeroCuenta,direccion)  
                               On source.nit = target.nit 
                               WHEN NOT MATCHED THEN INSERT (descripcion ,nit,numeroCuenta, direccion) 
                               Values (source.descripcion , source.nit, source.numeroCuenta,source.direccion)
                               WHEN MATCHED THEN UPDATE SET descripcion  = source.descripcion,
                                                            numeroCuenta = source.numeroCuenta, 
                                                              direccion  = source.direccion ;
                            ";



            cn.Open();
            using (SqlCommand cmd = new SqlCommand(query, cn))
            {

               
                cmd.Parameters.Add("@nit", SqlDbType.NVarChar).Value = fila["STCD1"].ToString();
                cmd.Parameters.Add("@numeroCuenta", SqlDbType.NVarChar).Value = fila["LIFNR"].ToString();
                cmd.Parameters.Add("@descripcion", SqlDbType.NVarChar).Value = fila["NAME1"].ToString();
                cmd.Parameters.Add("@direccion", SqlDbType.NVarChar).Value = fila["STRAS"].ToString();

                cmd.ExecuteNonQuery();
            
            }
            cn.Close();
            #endregion 
        }

          }
        public static void cargarPosicionesLibres()    
        {

            ECCDestinationConfig cfg = new ECCDestinationConfig();
            RfcDestinationManager.RegisterDestinationConfiguration(cfg);
            RfcDestination dest = RfcDestinationManager.GetDestination("TGQ");

            RfcRepository rfcRepository = dest.Repository;

             IRfcFunction rfcfunction = rfcRepository.CreateFunction("BAPI_REQUISITION_GETITEMS");
             rfcfunction.Invoke(dest);
             IRfcTable tblUserInfo = rfcfunction.GetTable("REQUISITION_ITEMS",true );
            DataTable dtbl = cfg.ConvertToDotNetTable(tblUserInfo);
            List<string> listadoColumnas = new List<string>();




            string query = @"Insert into CarguePosicionesLibresSinc(NUMERO_SOLPED,PREQ_ITEM,DOC_TYPE,CREATED_BY,CH_ON,PREQ_NAME,SHORT_TEXT,MATERIAL,
                                                                    MATERIAL_EXTERNAL,
                                                                    PUR_GROUP, PUR_MAT, PLANT,STORE_LOC,UNIT) 
            values (@NUMERO_SOLPED,@PREQ_ITEM,@DOC_TYPE,@CREATED_BY,@CH_ON,@PREQ_NAME,@SHORT_TEXT,@MATERIAL,@MATERIAL_EXTERNAL,
                    @PUR_GROUP,@PUR_MAT, @PLANT,@STORE_LOC,@UNIT)";
 

            SqlConnection cn = new SqlConnection(cadenaConexionDestino);
           
            cn.Open();
            using (SqlCommand cmd = new SqlCommand("TRUNCATE TABLE CarguePosicionesLibresSinc", cn))
            {
                cmd.ExecuteNonQuery();
            }
            cn.Close();

          
          
              foreach (DataRow fila in dtbl.Rows)
             {
                
                  

                     using (SqlCommand cmd = new SqlCommand(query, cn))
                     {
                      
                         cn.Open();
                         cmd.Parameters.Add("@NUMERO_SOLPED", SqlDbType.NVarChar).Value = fila["PREQ_NO"].ToString();
                         cmd.Parameters.Add("@PREQ_ITEM", SqlDbType.NVarChar).Value = fila["PREQ_ITEM"].ToString();
                         cmd.Parameters.Add("@DOC_TYPE", SqlDbType.NVarChar).Value = fila["DOC_TYPE"].ToString();
                         cmd.Parameters.Add("@CREATED_BY", SqlDbType.NVarChar).Value = fila["CREATED_BY"].ToString();
                         cmd.Parameters.Add("@CH_ON", SqlDbType.NVarChar).Value = "";
                         cmd.Parameters.Add("@PREQ_NAME", SqlDbType.NVarChar).Value = fila["PREQ_NAME"].ToString();
                         cmd.Parameters.Add("@SHORT_TEXT", SqlDbType.NVarChar).Value = fila["SHORT_TEXT"].ToString();
                         cmd.Parameters.Add("@MATERIAL", SqlDbType.NVarChar).Value = fila["MATERIAL"].ToString();
                         cmd.Parameters.Add("@MATERIAL_EXTERNAL", SqlDbType.NVarChar).Value = fila["MATERIAL_EXTERNAL"].ToString();

                         cmd.Parameters.Add("@PUR_GROUP", SqlDbType.NVarChar).Value = fila["PUR_GROUP"].ToString();
                         cmd.Parameters.Add("@PUR_MAT", SqlDbType.NVarChar).Value = fila["PUR_MAT"].ToString();
                         cmd.Parameters.Add("@PLANT", SqlDbType.NVarChar).Value = fila["PLANT"].ToString();
                         cmd.Parameters.Add("@STORE_LOC", SqlDbType.NVarChar).Value = fila["STORE_LOC"].ToString();
                         cmd.Parameters.Add("@UNIT", SqlDbType.NVarChar).Value = fila["UNIT"].ToString();

                       

                         cmd.ExecuteNonQuery();
                         cn.Close();
                    
                      
                 }
                  
             }


              query = string.Empty;
              query = @"MERGE   CarguePosicionesLibres AS TARGET  
              USING  CarguePosicionesLibresSinc AS SOURCE 
                  ON TARGET.NUMERO_SOLPED=SOURCE.NUMERO_SOLPED AND TARGET.PREQ_ITEM= SOURCE.PREQ_ITEM 
              WHEN NOT MATCHED THEN INSERT  (
                                                    NUMERO_SOLPED,PREQ_ITEM,DOC_TYPE,CREATED_BY,
                                                    CH_ON,PREQ_NAME,SHORT_TEXT,MATERIAL,MATERIAL_EXTERNAL,HABILITADO,
                                                    PUR_GROUP, PUR_MAT, PLANT,STORE_LOC,UNIT
                                             )
                VALUES (
                        SOURCE.NUMERO_SOLPED,SOURCE.PREQ_ITEM,SOURCE.DOC_TYPE,SOURCE.CREATED_BY,SOURCE.CH_ON,
                        SOURCE.PREQ_NAME,SOURCE.SHORT_TEXT,SOURCE.MATERIAL,SOURCE.MATERIAL_EXTERNAL,1,
                        SOURCE.PUR_GROUP, SOURCE.PUR_MAT, SOURCE.PLANT,SOURCE.STORE_LOC,SOURCE.UNIT
                        )
              WHEN NOT MATCHED BY SOURCE THEN UPDATE SET TARGET.HABILITADO= 0 
                ;";

              cn.Open();
              using (SqlCommand cmd = new SqlCommand(query, cn))
              {
                  cmd.ExecuteNonQuery();
              }
              cn.Close();

        }
        public static void enviarSolicitudCotizacion() 
       {

             ECCDestinationConfig cfg = new ECCDestinationConfig();
             RfcDestinationManager.RegisterDestinationConfiguration(cfg);
             RfcDestination dest = RfcDestinationManager.GetDestination("TGQ");

             RfcRepository rfcRepository = dest.Repository;

             IRfcFunction rfcfunction = rfcRepository.CreateFunction("ZOFERTA_NEW");
             rfcfunction.SetValue("LIFNR", "0000900040"); /*CUENTA*/
             rfcfunction.SetValue("STCD1", "8110077294"); /*NIT*/
             IRfcTable tblUserInfo = rfcfunction.GetTable("BAPIEBAN");
            
             String query = @"Select NUMERO_SOLPED PREQ_NO,  PREQ_ITEM,DOC_TYPE,
                                    PUR_GROUP,convert(date,GETDATE()) AS PREQ_DATE ,SHORT_TEXT,MATERIAL , 
                                    PUR_MAT,   PLANT, STORE_LOC, 10 QUANTITY ,UNIT, 11000 NETPR ,
                                    10* 11000 C_AMT_BAPI
                             From CarguePosicionesLibres where NUMERO_SOLPED= '0010066646'";
             SqlConnection cn = new SqlConnection(cadenaConexionDestino);  
            cn.Open();
              using (SqlCommand cmd = new SqlCommand(query, cn))
        {

            SqlDataReader reader = cmd.ExecuteReader();

            DataTable dt = new DataTable();
            dt.Load(reader);

            foreach (DataRow row in dt.Rows)
            {
                 IRfcStructure structImport = dest.Repository.GetStructureMetadata("ZBAPIEBAN").CreateStructure();

                 structImport.SetValue("PREQ_NO", row["PREQ_NO"].ToString());
                 structImport.SetValue("PREQ_ITEM", row["PREQ_ITEM"].ToString());
                 structImport.SetValue("DOC_TYPE", row["DOC_TYPE"].ToString());

                 structImport.SetValue("PUR_GROUP", row["PUR_GROUP"].ToString());
                 structImport.SetValue("PREQ_DATE",  row["PREQ_DATE"]);
                 structImport.SetValue("SHORT_TEXT", row["SHORT_TEXT"].ToString());
                 structImport.SetValue("MATERIAL", row["MATERIAL"].ToString());
                 structImport.SetValue("PUR_MAT", row["PUR_MAT"].ToString());
                 structImport.SetValue("PLANT", row["PLANT"].ToString());
                 structImport.SetValue("STORE_LOC", row["STORE_LOC"].ToString());
                 structImport.SetValue("QUANTITY", row["QUANTITY"].ToString());
                 structImport.SetValue("UNIT", row["UNIT"].ToString());
                 structImport.SetValue("NETPR", row["NETPR"].ToString());
                 structImport.SetValue("C_AMT_BAPI", row["C_AMT_BAPI"].ToString());

                tblUserInfo.Insert(structImport); 
            }
       }
        cn.Close();
         
            
                 rfcfunction.Invoke(dest);
                 String s = rfcfunction.GetString("I_ORT01");
            
      }


        public static void subirCotizacion(string LIFNR, string STCD1, int idSolicitudSapCap )
        {
            ECCDestinationConfig cfg = new ECCDestinationConfig();
            RfcDestinationManager.RegisterDestinationConfiguration(cfg);
            RfcDestination dest = RfcDestinationManager.GetDestination("TGQ");

            RfcRepository rfcRepository = dest.Repository;

            IRfcFunction rfcfunction = rfcRepository.CreateFunction("ZOFERTA_NEW");
            rfcfunction.SetValue("LIFNR", LIFNR); /*CUENTA*/
            rfcfunction.SetValue("STCD1", STCD1); /*NIT*/
            IRfcTable tblUserInfo = rfcfunction.GetTable("BAPIEBAN");

            String query = @"Select * from V_POSICIONES_PARA_SAP WHERE  idSolicitudSapCap= " + idSolicitudSapCap + "";
            SqlConnection cn = new SqlConnection(cadenaConexionDestino);
            cn.Open();
            using (SqlCommand cmd = new SqlCommand(query, cn))
            {

                SqlDataReader reader = cmd.ExecuteReader();

                DataTable dt = new DataTable();
                dt.Load(reader);

                foreach (DataRow row in dt.Rows)
                {
                    IRfcStructure structImport = dest.Repository.GetStructureMetadata("ZBAPIEBAN").CreateStructure();

                    structImport.SetValue("PREQ_NO", row["PREQ_NO"].ToString());
                    structImport.SetValue("PREQ_ITEM", row["PREQ_ITEM"].ToString());
                    structImport.SetValue("DOC_TYPE", row["DOC_TYPE"].ToString());

                    structImport.SetValue("PUR_GROUP", row["PUR_GROUP"].ToString());
                    structImport.SetValue("PREQ_DATE", row["PREQ_DATE"]);
                    structImport.SetValue("SHORT_TEXT", row["SHORT_TEXT"].ToString());
                    structImport.SetValue("MATERIAL", row["MATERIAL"].ToString());
                    structImport.SetValue("PUR_MAT", row["PUR_MAT"].ToString());
                    structImport.SetValue("PLANT", row["PLANT"].ToString());
                    structImport.SetValue("STORE_LOC", row["STORE_LOC"].ToString());
                    structImport.SetValue("QUANTITY", row["QUANTITY"].ToString());
                    structImport.SetValue("UNIT", row["UNIT"].ToString());
                    structImport.SetValue("NETPR", row["NETPR"].ToString());
                    structImport.SetValue("C_AMT_BAPI", row["C_AMT_BAPI"].ToString());

                    tblUserInfo.Insert(structImport);
                }

                
            }

            query = "update SolicitudCotizacionSapCap set enviadosap = 1 WHERE  id= " + idSolicitudSapCap;
            using (SqlCommand cmd = new SqlCommand(query, cn))
            {
                cmd.ExecuteNonQuery(); 
            }

            cn.Close();


            rfcfunction.Invoke(dest);
            String s = rfcfunction.GetString("I_ORT01");
            
        }

        public static void subirCotizacionesSap()
        {

            SqlConnection cn = new SqlConnection(cadenaConexionDestino);  
            cn.Open();
            string query = "SELECT distinct idSolicitudSapCap,LIFNR,STCD1 FROM V_POSICIONES_PARA_SAP";
            using (SqlCommand cmd = new SqlCommand(query, cn))
            {

                SqlDataReader reader = cmd.ExecuteReader();

                DataTable dt = new DataTable();
                dt.Load(reader);

                foreach (DataRow row in dt.Rows)
                {
                    subirCotizacion(row["LIFNR"].ToString(), row["STCD1"].ToString(), int.Parse(row["idSolicitudSapCap"].ToString())) ; 
                    
                }
            }
        
        }
        
        public  static void  cargarPosiciones()        
    {
        ECCDestinationConfig cfg = new ECCDestinationConfig();
        RfcDestinationManager.RegisterDestinationConfiguration(cfg);
        RfcDestination dest = RfcDestinationManager.GetDestination("TGQ");

        RfcRepository rfcRepository = dest.Repository;



        IRfcFunction rfcfunction = rfcRepository.CreateFunction("BAPI_PR_GETDETAIL");
        rfcfunction.SetValue("NUMBER", "0010000135");
        rfcfunction.Invoke(dest);
        IRfcTable tblUserInfo = rfcfunction.GetTable("PRITEM");
        DataTable dtbl = cfg.ConvertToDotNetTable(tblUserInfo);
        List<string> listadoColumnas = new List<string>();

        foreach (DataColumn cl in dtbl.Columns)
        {
            listadoColumnas.Add(cl.ColumnName);

        }
        SqlConnection cn = new SqlConnection(cadenaConexionDestino);
        String query = "Insert into CarguePosicionesLibres(NUMERO_SOLPED,PREQ_ITEM,DOC_TYPE,CREATED_BY,CH_ON,PREQ_NAME,SHORT_TEXT,MATERIAL,MATERIAL_EXTERNAL) ";
        query = query + "values (@NUMERO_SOLPED,@PREQ_ITEM,@DOC_TYPE,@CREATED_BY,@CH_ON,@PREQ_NAME,@SHORT_TEXT,@MATERIAL,@MATERIAL_EXTERNAL)";



        cn.Open();
        using (SqlCommand cmd = new SqlCommand("TRUNCATE TABLE CarguePosicionesLibres", cn))
        {
            cmd.ExecuteNonQuery();
        }
        cn.Close();



        foreach (DataRow fila in dtbl.Rows)
        {
            using (SqlCommand cmd = new SqlCommand(query, cn))
            {

                cn.Open();
                cmd.Parameters.Add("@NUMERO_SOLPED", SqlDbType.NVarChar).Value = "0010000135";
                cmd.Parameters.Add("@PREQ_ITEM", SqlDbType.NVarChar).Value = fila["PREQ_ITEM"].ToString();
                cmd.Parameters.Add("@DOC_TYPE", SqlDbType.NVarChar).Value = fila["DOC_TYPE"].ToString();
                cmd.Parameters.Add("@CREATED_BY", SqlDbType.NVarChar).Value = fila["CREATED_BY"].ToString();
                cmd.Parameters.Add("@CH_ON", SqlDbType.NVarChar).Value = fila["CH_ON"].ToString();
                cmd.Parameters.Add("@PREQ_NAME", SqlDbType.NVarChar).Value = fila["PREQ_NAME"].ToString();
                cmd.Parameters.Add("@SHORT_TEXT", SqlDbType.NVarChar).Value = fila["SHORT_TEXT"].ToString();
                cmd.Parameters.Add("@MATERIAL", SqlDbType.NVarChar).Value = fila["MATERIAL"].ToString();
                cmd.Parameters.Add("@MATERIAL_EXTERNAL", SqlDbType.NVarChar).Value = fila["MATERIAL_EXTERNAL"].ToString();


                cmd.ExecuteNonQuery();
                cn.Close();
            }
        }
    
    }
        
        static void Main(string[] args)
        {

            subirCotizacionesSap(); 
         
        }
        
         

    }
}


