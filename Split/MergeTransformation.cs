using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.Azure.Management.DataFactories.Models;

namespace Phidiax.Azure.ComponentExamples.Split
{
    public class MergeTransformation : IDotNetActivity
    {
        public IDictionary<string, string> Execute(IEnumerable<LinkedService> linkedServices, IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            var inDS = Helper.DatasetHelper.GetInputDataset(activity, linkedServices, datasets);
            var outDS = Helper.DatasetHelper.GetOutputDatasetShell(activity, linkedServices, datasets);

            //Settings:
            //MergeC# : C#
            //OutputColName: T#C#

            List<System.Data.DataColumn> lstParentColumns = new List<System.Data.DataColumn>();
            List<System.Data.DataColumn> lstChildColumns = new List<System.Data.DataColumn>();

            //Build column relationship list
            foreach (var mergeSetting in ((DotNetActivity)(activity.TypeProperties)).ExtendedProperties.Where(ep=>ep.Key.Contains("Merge")))
            {
                lstParentColumns.Add(inDS.Tables[0].Columns[int.Parse(mergeSetting.Key.Replace("MergeC",""))]);
                lstChildColumns.Add(inDS.Tables[1].Columns[int.Parse(mergeSetting.Value.Replace("C",""))]);
            }


            System.Data.DataRelation dr = new System.Data.DataRelation("Merge", lstParentColumns.ToArray(), lstChildColumns.ToArray(), false);

            inDS.Relations.Add(dr);
            
            //Loop the child, not the parent                
            foreach (System.Data.DataRow r in inDS.Tables[1].Rows)
            {
                System.Data.DataRow lstOutRow = outDS.Tables[0].NewRow();

                foreach (var colSetting in ((DotNetActivity)(activity.TypeProperties)).ExtendedProperties.Where(ep => !ep.Key.Contains("Merge") && !ep.Key.Contains("SliceStart")))
                {
                    if (colSetting.Value.Contains("T1"))
                        lstOutRow[colSetting.Key] = r[int.Parse(colSetting.Value.Replace("T1C", ""))];
                    else
                        lstOutRow[colSetting.Key] = r.GetParentRow("Merge")[int.Parse(colSetting.Value.Replace("T0C", ""))];
                }

                outDS.Tables[0].Rows.Add(lstOutRow);
            }
            

            Helper.DatasetHelper.WriteOutputDataset(activity, linkedServices, datasets, outDS);

            return new Dictionary<string, string>();
        }
    }
}
