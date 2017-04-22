using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Runtime;
using Microsoft.Azure.Management.DataFactories.Models;

namespace Phidiax.Azure.ComponentExamples.Split
{
    public class ConditionalSplitTransformation : IDotNetActivity
    {
        public ConditionalSplitTransformation() { }

        public IDictionary<string, string> Execute(IEnumerable<LinkedService> linkedServices, 
                                                   IEnumerable<Dataset> datasets, 
                                                   Activity activity, 
                                                   IActivityLogger logger)
        {
            //Output#: Dataset Compatible Filter string
            //# of formulas must match # datasets
            //Single input expected
            var inDS = Helper.DatasetHelper.GetInputDatatable(activity, linkedServices, datasets);
            var outDS = Helper.DatasetHelper.GetOutputDatasetShell(activity, linkedServices, datasets);

            foreach (var o in activity.Outputs)
            {
                System.Data.DataView dv = new System.Data.DataView(inDS);
                dv.RowFilter = ((DotNetActivity)(activity.TypeProperties)).ExtendedProperties.Single(ep => ep.Key == string.Format("DatasetCondition{0}", activity.Outputs.IndexOf(o))).Value;
                
                foreach (System.Data.DataRow r in dv.ToTable().Rows)
                {
                    outDS.Tables[activity.Outputs.IndexOf(o)].Rows.Add(r.ItemArray);
                }
            }

            Helper.DatasetHelper.WriteOutputDataset(activity, linkedServices, datasets, outDS);

            return new Dictionary<string, string>();
        }
    }
}
