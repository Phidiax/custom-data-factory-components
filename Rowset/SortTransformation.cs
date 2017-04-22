using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Models;
using Microsoft.Azure.Management.DataFactories.Runtime;

namespace Phidiax.Azure.ComponentExamples.Rowset
{
    public class SortTransformation : Microsoft.Azure.Management.DataFactories.Runtime.IDotNetActivity
    {
        public IDictionary<string, string> Execute(IEnumerable<LinkedService> linkedServices, IEnumerable<Dataset> datasets, Activity activity, IActivityLogger logger)
        {
            //Settings in extended items:
            //1) List of columns to operate on
            //  a) Sort Column
            //  b) Sort Type
            //  c) Sort Order

            var inDS = Helper.DatasetHelper.GetInputDatatable(activity, linkedServices, datasets);
            var outDS = Helper.DatasetHelper.GetOutputDatasetShell(activity, linkedServices, datasets);

            foreach (var o in activity.Outputs)
            {
                System.Data.DataView dv = new System.Data.DataView(inDS); 
                dv.Sort = ((DotNetActivity)(activity.TypeProperties)).ExtendedProperties.Single(ep => ep.Key == string.Format("DatasetSort{0}", activity.Outputs.IndexOf(o))).Value;
                foreach (System.Data.DataRow r in dv.ToTable().Rows)
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
