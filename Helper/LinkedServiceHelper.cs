using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.Management.DataFactories.Models;

namespace Phidiax.Azure.ComponentExamples.Helper
{
    public static class LinkedServiceHelper
    {

        public static LinkedService GetInputLinkedService(Activity dnActivity, 
                                                          IEnumerable<LinkedService> linkedServices,
                                                          IEnumerable<Dataset> datasets)
        {
            return linkedServices.First(f => f.Name == datasets.Single(l => l.Name == dnActivity.Inputs.First().Name).Properties.LinkedServiceName);
        }

        public static LinkedService GetOutputLinkedService(Activity dnActivity, 
                                                           IEnumerable<LinkedService> linkedServices,
                                                           IEnumerable<Dataset> datasets)
        {
            return linkedServices.First(f => f.Name == datasets.Single(l => l.Name == dnActivity.Outputs.First().Name).Properties.LinkedServiceName);
        }
    }
}
