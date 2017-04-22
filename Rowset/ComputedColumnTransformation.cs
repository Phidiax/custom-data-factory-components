using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;

namespace Phidiax.Azure.ComponentExamples.Rowset
{
    public class ComputedColumnTransformation : IDotNetActivity
    {
        public IDictionary<string, string> Execute(IEnumerable<LinkedService> linkedServices, IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            var inDS = Helper.DatasetHelper.GetInputDatatable(activity, linkedServices, datasets);
            var outDS = Helper.DatasetHelper.GetOutputDatasetShell(activity, linkedServices, datasets);

            //Format of specified fields:
            //Tbl#Col#: Formula Expression

            foreach (var o in activity.Outputs)
            {
                var lstComputedColumns = ((DotNetActivity)(activity.TypeProperties)).ExtendedProperties.Where(ep => ep.Key.Contains(string.Format("Tbl{0}", activity.Outputs.IndexOf(o))));

                foreach (var col in lstComputedColumns)
                {
                    int colNo = int.Parse(col.Key.Replace(string.Format("Tbl{0}Col", activity.Outputs.IndexOf(o)), ""));
                    outDS.Tables[activity.Outputs.IndexOf(o)].Columns[colNo].Expression = col.Value;
                }

                foreach (System.Data.DataRow r in inDS.Rows)
                {
                    outDS.Tables[activity.Outputs.IndexOf(o)].Rows.Add(r.ItemArray);
                }
            }

            Helper.DatasetHelper.WriteOutputDataset(activity, linkedServices, datasets, outDS);



            //Per documentation, this isn't used yet
            return new Dictionary<string, string>();
        }
    }
}
