using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace Phidiax.Azure.ComponentExamples.Helper
{
    public static class DatasetHelper
    {
        public static System.Data.DataSet GetOutputDatasetShell(Activity dnActivity,
                                                  IEnumerable<LinkedService> linkedServices,
                                                  IEnumerable<Dataset> datasets)
        {
            //SQL or Azure Blob CSV only
            var outLS = LinkedServiceHelper.GetOutputLinkedService(dnActivity, linkedServices, datasets);
            System.Data.DataSet dsRtn = new System.Data.DataSet();

            //Figure out which Type
            switch (outLS.Properties.Type)
            {
                case "AzureStorage":
                    foreach (var ds in dnActivity.Outputs)
                    {
                        System.Data.DataTable dtNew = new System.Data.DataTable(ds.Name);

                        foreach (var d in datasets.Single(d => d.Name == ds.Name).Properties.Structure)
                        {
                            Type tCol = typeof(string);

                            switch (d.Type)
                            {
                                case "Boolean":
                                    tCol = typeof(bool);
                                    break;
                                case "Decimal":
                                    tCol = typeof(decimal);
                                    break;
                                case "Enum":
                                    tCol = typeof(Enum);
                                    break;
                                case "Guid":
                                    tCol = typeof(Guid);
                                    break;
                                case "Int16":
                                    tCol = typeof(Int16);
                                    break;
                                case "Int64":
                                    tCol = typeof(Int64);
                                    break;
                                case "Int32":
                                    tCol = typeof(Int32);
                                    break;
                                case "Datetime":
                                    tCol = typeof(DateTime);
                                    break;
                                case "DateTimeOffset":
                                    tCol = typeof(DateTimeOffset);
                                    break;
                                case "Double":
                                    tCol = typeof(double);
                                    break;
                                case "Single":
                                    tCol = typeof(Single);
                                    break;
                                case "Timespan":
                                    tCol = typeof(TimeSpan);
                                    break;
                                    

                            }
                            dtNew.Columns.Add(d.Name, tCol);
                        }

                        dsRtn.Tables.Add(dtNew);
                    }
                    break;
                case "AzureSqlDatabase":
                    System.Data.SqlClient.SqlConnection scOutput = new System.Data.SqlClient.SqlConnection(((AzureSqlDatabaseLinkedService)outLS.Properties.TypeProperties).ConnectionString);
                    System.Data.SqlClient.SqlCommand commOutput = new System.Data.SqlClient.SqlCommand();
                    
                    foreach (var t in dnActivity.Outputs)
                    {
                        AzureSqlTableDataset astOutput = datasets.Single(d => d.Name == t.Name).Properties.TypeProperties as AzureSqlTableDataset;
                        commOutput.Connection = scOutput;
                        commOutput.CommandType = System.Data.CommandType.TableDirect;
                        commOutput.CommandText = astOutput.TableName;

                        System.Data.SqlClient.SqlDataAdapter sdaInput = new System.Data.SqlClient.SqlDataAdapter(commOutput);

                        sdaInput.FillSchema(dsRtn, System.Data.SchemaType.Source);
                    }

                    break;
                default:
                    throw new NotImplementedException();
            }

            return dsRtn;
        }

        public static void WriteOutputDataset(Activity dnActivity,
                                                          IEnumerable<LinkedService> linkedServices,
                                                          IEnumerable<Dataset> datasets, System.Data.DataSet dsOutput)
        {
            //SQL or Azure Blob CSV only
            var outLS = LinkedServiceHelper.GetOutputLinkedService(dnActivity, linkedServices, datasets);

            //Figure out which Type
            switch (outLS.Properties.Type)
            {
                case "AzureStorage":
                    CloudStorageAccount outputStorageAccount = CloudStorageAccount.Parse(((AzureStorageLinkedService)outLS.Properties.TypeProperties).ConnectionString);
                    CloudBlobClient outputClient = outputStorageAccount.CreateCloudBlobClient();
                    
                    foreach (var t in dnActivity.Outputs)
                    {
                        AzureBlobDataset abdOutput = datasets.Single(d => d.Name == t.Name).Properties.TypeProperties as AzureBlobDataset;
                        CloudBlobContainer container = outputClient.GetContainerReference(abdOutput.FolderPath.Split('/').First());
                        string sFolder = abdOutput.FolderPath.Replace(abdOutput.FolderPath.Split('/').First(), "");
                        CloudBlockBlob cbbOutputFile = container.GetBlockBlobReference(sFolder.Length > 0 ? sFolder + "/" + abdOutput.FileName : abdOutput.FileName);
                        
                        
                        using (System.IO.MemoryStream ms = new System.IO.MemoryStream())
                        using (var swOutput = new System.IO.StreamWriter(ms))
                        {
                            System.Data.DataTable dt = dsOutput.Tables[t.Name];

                            foreach (System.Data.DataRow r in dt.Rows)
                            {
                                foreach (System.Data.DataColumn c in dt.Columns)
                                {
                                    if (dt.Columns.IndexOf(c) == 0)
                                        swOutput.Write("\"");

                                    swOutput.Write(r[c]);

                                    if (dt.Columns.IndexOf(c) != dt.Columns.Count - 1)
                                        swOutput.Write("\",\"");
                                    else
                                        swOutput.WriteLine("\"");
                                }
                            }
                            
                            swOutput.Flush();
                            ms.Position = 0;
                            cbbOutputFile.UploadFromStream(ms, null, null, null);
                        }

                        
                    }

                    break;
                case "AzureSqlDatabase":
                    foreach (var t in dnActivity.Outputs)
                    {
                        AzureSqlTableDataset astOutput = datasets.Single(d => d.Name == t.Name).Properties.TypeProperties as AzureSqlTableDataset;
                        System.Data.SqlClient.SqlConnection scOutput = new System.Data.SqlClient.SqlConnection(((AzureSqlDatabaseLinkedService)outLS.Properties.TypeProperties).ConnectionString);
                        System.Data.SqlClient.SqlCommand commOutput = new System.Data.SqlClient.SqlCommand();

                        commOutput.Connection = scOutput;
                        commOutput.CommandType = System.Data.CommandType.TableDirect;
                        commOutput.CommandText = astOutput.TableName;

                        System.Data.SqlClient.SqlDataAdapter sdaInput = new System.Data.SqlClient.SqlDataAdapter(commOutput);

                        sdaInput.Update(dsOutput.Tables[t.Name]);
                    }
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        public static System.Data.DataTable GetInputDatatableShell(Activity dnActivity,
                                                          IEnumerable<LinkedService> linkedServices,
                                                          IEnumerable<Dataset> datasets)
        {
            //SQL or Azure Blob CSV only
            var inLS = LinkedServiceHelper.GetInputLinkedService(dnActivity, linkedServices, datasets);
            System.Data.DataTable dsRtn = new System.Data.DataTable(dnActivity.Inputs.First().Name);

            //Figure out which Type
            switch (inLS.Properties.Type)
            {
                case "AzureStorage":
                    foreach (var d in datasets.Single(d => d.Name == dnActivity.Inputs.First().Name).Properties.Structure)
                    {
                        dsRtn.Columns.Add(d.Name);
                    }
                    break;
                case "AzureSqlDatabase":
                    AzureSqlTableDataset astInput = datasets.Single(d=>d.Name == dnActivity.Inputs.First().Name).Properties.TypeProperties as AzureSqlTableDataset;
                    System.Data.SqlClient.SqlConnection scInput = new System.Data.SqlClient.SqlConnection(((AzureSqlDatabaseLinkedService)inLS.Properties.TypeProperties).ConnectionString);
                    System.Data.SqlClient.SqlCommand commInput = new System.Data.SqlClient.SqlCommand();
                    
                    commInput.Connection = scInput;
                    commInput.CommandType = System.Data.CommandType.Text;
                    commInput.CommandText = string.Format("SELECT * FROM [{0}]", astInput.TableName);

                    System.Data.SqlClient.SqlDataAdapter sdaInput = new System.Data.SqlClient.SqlDataAdapter(commInput);

                    sdaInput.FillSchema(dsRtn, System.Data.SchemaType.Source);
                    break;
                default:
                    throw new NotImplementedException();
            }

            return dsRtn;
        }

        public static System.Data.DataTable GetInputDatatable(Activity dnActivity,
                                                          IEnumerable<LinkedService> linkedServices,
                                                          IEnumerable<Dataset> datasets)
        {
            //SQL or Azure Blob CSV only
            var inLS = LinkedServiceHelper.GetInputLinkedService(dnActivity, linkedServices, datasets);
            System.Data.DataTable dsRtn = GetInputDatatableShell(dnActivity, linkedServices, datasets);

            //Figure out which Type
            switch (inLS.Properties.Type)
            {
                case "AzureStorage":
                    CloudStorageAccount inputStorageAccount = CloudStorageAccount.Parse(((AzureStorageLinkedService)inLS.Properties.TypeProperties).ConnectionString);
                    CloudBlobClient inputClient = inputStorageAccount.CreateCloudBlobClient();
                    AzureBlobDataset abdInput = datasets.Single(d=>d.Name == dnActivity.Inputs.First().Name).Properties.TypeProperties as AzureBlobDataset;
                    CloudBlockBlob cbbInputFile = new CloudBlockBlob(new Uri(inputStorageAccount.BlobEndpoint.AbsoluteUri + abdInput.FolderPath + "/" + abdInput.FileName));

                    System.IO.MemoryStream ms = new System.IO.MemoryStream();
                    cbbInputFile.DownloadToStream(ms);
                    ms.Position = 0;

                    using (Microsoft.VisualBasic.FileIO.TextFieldParser tfp = new Microsoft.VisualBasic.FileIO.TextFieldParser(ms))
                    {
                        tfp.TextFieldType = Microsoft.VisualBasic.FileIO.FieldType.Delimited;
                        tfp.SetDelimiters(",");
                        while (!tfp.EndOfData)
                        {
                            string[] fields = tfp.ReadFields();
                            dsRtn.LoadDataRow(fields, true);
                        }
                    }

                    break;
                case "AzureSqlDatabase":
                    AzureSqlTableDataset astInput = datasets.Single(d=>d.Name == dnActivity.Inputs.First().Name).Properties.TypeProperties as AzureSqlTableDataset;
                    System.Data.SqlClient.SqlConnection scInput = new System.Data.SqlClient.SqlConnection(((AzureSqlDatabaseLinkedService)inLS.Properties.TypeProperties).ConnectionString);
                    System.Data.SqlClient.SqlCommand commInput = new System.Data.SqlClient.SqlCommand();
                    
                    commInput.Connection = scInput;
                    commInput.CommandType = System.Data.CommandType.Text;
                    commInput.CommandText = string.Format("SELECT * FROM [{0}]", astInput.TableName);

                    System.Data.SqlClient.SqlDataAdapter sdaInput = new System.Data.SqlClient.SqlDataAdapter(commInput);

                    sdaInput.Fill(dsRtn);

                    break;
                default:
                    throw new NotImplementedException();
            }

            return dsRtn;
        }

        public static System.Data.DataSet GetInputDatasetShell(Activity dnActivity,
                                                          IEnumerable<LinkedService> linkedServices,
                                                          IEnumerable<Dataset> datasets)
        {
            //SQL or Azure Blob CSV only
            var inLS = LinkedServiceHelper.GetInputLinkedService(dnActivity, linkedServices, datasets);
            System.Data.DataSet dsRtn = new System.Data.DataSet();

            //Figure out which Type
            switch (inLS.Properties.Type)
            {
                case "AzureStorage":
                    foreach (var ds in dnActivity.Inputs)
                    {
                        var curTbl = dsRtn.Tables.Add(ds.Name);

                        foreach (var d in datasets.First(ds1=>ds1.Name == ds.Name).Properties.Structure)
                        {
                            curTbl.Columns.Add(d.Name);
                        }
                    }
                    break;
                case "AzureSqlDatabase":
                    foreach (var ds in datasets)
                    {
                        var curTbl = dsRtn.Tables.Add(ds.Name);

                        AzureSqlTableDataset astInput = ds.Properties.TypeProperties as AzureSqlTableDataset;
                        System.Data.SqlClient.SqlConnection scInput = new System.Data.SqlClient.SqlConnection(((AzureSqlDatabaseLinkedService)inLS.Properties.TypeProperties).ConnectionString);
                        System.Data.SqlClient.SqlCommand commInput = new System.Data.SqlClient.SqlCommand();

                        commInput.Connection = scInput;
                        commInput.CommandType = System.Data.CommandType.Text;
                        commInput.CommandText = string.Format("SELECT * FROM [{0}]", astInput.TableName);

                        System.Data.SqlClient.SqlDataAdapter sdaInput = new System.Data.SqlClient.SqlDataAdapter(commInput);

                        sdaInput.FillSchema(curTbl, System.Data.SchemaType.Source);
                    }
                    break;
                default:
                    throw new NotImplementedException();
            }

            return dsRtn;
        }

        public static System.Data.DataSet GetInputDataset(Activity dnActivity,
                                                          IEnumerable<LinkedService> linkedServices,
                                                          IEnumerable<Dataset> datasets)
        {
            //SQL or Azure Blob CSV only
            var inLS = LinkedServiceHelper.GetInputLinkedService(dnActivity, linkedServices, datasets);
            System.Data.DataSet dsRtn = GetInputDatasetShell(dnActivity, linkedServices, datasets);

            //Figure out which Type
            switch (inLS.Properties.Type)
            {
                case "AzureStorage":
                    CloudStorageAccount inputStorageAccount = CloudStorageAccount.Parse(((AzureStorageLinkedService)inLS.Properties.TypeProperties).ConnectionString);
                    CloudBlobClient inputClient = inputStorageAccount.CreateCloudBlobClient();

                    foreach (var ds in dnActivity.Inputs)
                    {
                        var curTbl = dsRtn.Tables[ds.Name];

                        AzureBlobDataset abdInput = datasets.First(d=>d.Name == ds.Name).Properties.TypeProperties as AzureBlobDataset;
                        CloudBlockBlob cbbInputFile = new CloudBlockBlob(new Uri(inputStorageAccount.BlobEndpoint.AbsoluteUri + abdInput.FolderPath + "/" + abdInput.FileName));

                        System.IO.MemoryStream ms = new System.IO.MemoryStream();
                        cbbInputFile.DownloadToStream(ms);
                        ms.Position = 0;

                        using (Microsoft.VisualBasic.FileIO.TextFieldParser tfp = new Microsoft.VisualBasic.FileIO.TextFieldParser(ms))
                        {
                            tfp.TextFieldType = Microsoft.VisualBasic.FileIO.FieldType.Delimited;
                            tfp.SetDelimiters(",");
                            while (!tfp.EndOfData)
                            {
                                string[] fields = tfp.ReadFields();
                                curTbl.LoadDataRow(fields, true);
                            }
                        }
                    }
                    break;
                case "AzureSqlDatabase":
                    System.Data.SqlClient.SqlConnection scInput = new System.Data.SqlClient.SqlConnection(((AzureSqlDatabaseLinkedService)inLS.Properties.TypeProperties).ConnectionString);

                    foreach (var ds in datasets)
                    {
                        var curTbl = dsRtn.Tables[ds.Name];

                        AzureSqlTableDataset astInput = ds.Properties.TypeProperties as AzureSqlTableDataset;

                        System.Data.SqlClient.SqlCommand commInput = new System.Data.SqlClient.SqlCommand();

                        commInput.Connection = scInput;
                        commInput.CommandType = System.Data.CommandType.Text;
                        commInput.CommandText = string.Format("SELECT * FROM [{0}]", astInput.TableName);

                        System.Data.SqlClient.SqlDataAdapter sdaInput = new System.Data.SqlClient.SqlDataAdapter(commInput);

                        sdaInput.Fill(curTbl);
                    }
                    break;
                default:
                    throw new NotImplementedException();
            }

            return dsRtn;
        }
    }
}
