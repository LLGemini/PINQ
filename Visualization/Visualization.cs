using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using PINQ;
//using LinqToDryad;

namespace Visualization
{
    public class Program
    {
        #region Supporting global variables, methods, and classes.
        public class PINQAgentLogger : PINQAgent
        {
            string name;
            double total;
            public override bool apply(double epsilon)
            {
                total += epsilon;
                Console.WriteLine("**privacy change**\tdelta: " + epsilon.ToString("0.00") + "\ttotal: " + total.ToString("0.00") + "\t(" + name + ")");

                return true;
            }

            public PINQAgentLogger(string n) { name = n; }
        }

        public static StreamWriter outputStream;        // .html file for output.
        public static HashSet<string> locations;        // valid locations for data.

        public static void WriteHeader(string query)
        {
            outputStream = new StreamWriter("Examples\\" + query + ".html");

            outputStream.WriteLine("<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\">");
            outputStream.WriteLine("<html>");
            outputStream.WriteLine("   <head>");
            outputStream.WriteLine("   <title>{0}</title>", query);
            outputStream.WriteLine("      <meta http-equiv=\"Content-Type\" content=\"text/html; charset=utf-8\">");
            outputStream.WriteLine("      <script type=\"text/javascript\" src=\"http://dev.virtualearth.net/mapcontrol/mapcontrol.ashx?v=6\"></script>");
            outputStream.WriteLine("      <script type=\"text/javascript\">");
            outputStream.WriteLine("      var map = null;");
            outputStream.WriteLine("      var shape = null;");
            outputStream.WriteLine("      function GetMap()");
            outputStream.WriteLine("      {");
            outputStream.WriteLine("         map = new VEMap('myMap');");
            outputStream.WriteLine("         var icon = \"<div style='font-size:8px;font-weight:bold;border:solid 1px Black;background-color:green;width:8px;'></div>\";");
            outputStream.WriteLine("         map.LoadMap();");
            outputStream.WriteLine("");
        }
        public static void WriteRecord(string coordinates)
        {
            double lat = 0.0;
            double lon = 0.0;
            double rad = 180.0;

            for (int i = 0; i < coordinates.Length / 2; i++)
            {
                rad /= 2.0;
                lat += (coordinates[2 * i + 0] == '1') ? rad : -rad;
                lon += (coordinates[2 * i + 1] == '1') ? rad : -rad;
            }

            var random = new System.Random();

            lat += rad * 2.0 * (random.NextDouble() - 0.5);
            lon += rad * 2.0 * (random.NextDouble() - 0.5);

            outputStream.WriteLine("shape = new VEShape(VEShapeType.Pushpin, new VELatLong({0}, {1}));", lat / 2.0, lon);
            outputStream.WriteLine("shape.SetCustomIcon(icon);");
            outputStream.WriteLine("map.AddShape(shape);");
        }
        public static void WriteFooter()
        {
            outputStream.WriteLine("");
            outputStream.WriteLine("       }");
            outputStream.WriteLine("      </script>");
            outputStream.WriteLine("   </head>");
            outputStream.WriteLine("   <body onload=\"GetMap();\">");
            outputStream.WriteLine("      <div id='myMap' style=\"position:relative; width:1024px; height:768px;\"></div>");
            outputStream.WriteLine("   </body>");
            outputStream.WriteLine("</html>");

            outputStream.Flush();
            outputStream.Close();
        }

        // recursively partitions input by cycling through the coordinates of each input.
        public static void Histogram(PINQueryable<double[]> input, int count, string prefix)
        {
            #region Initialize valid locations, if necessary.
            if (locations == null)
            {
                locations = new HashSet<string>();
                foreach (var line in File.ReadAllLines("LocationsHash.txt"))
                    foreach (var length in Enumerable.Range(0, 34))
                        locations.Add(line.Substring(0, length));
            }
            #endregion

            if (count == 0)
                return;

            else if (locations.Contains(prefix + '0') && locations.Contains(prefix + '1'))
            {
                input = input.Materialize(); // materializes result; prevents redundant computation

                // partition the data by sign of the first coordinate, compute the fraction in each part.
                var parts = input.Partition<char>(new char[] { '0', '1' }, x => (x[0] > 0.0) ? '1' : '0');
                var delta = input.NoisyAverage(1.0 / (prefix.Length + 1), x => (x[0] == 0.0) ? 0.0 : x[0] / Math.Abs(x[0])) * count;

                // prepare each subpart for recursive invocations (switch coords and rescale)
                var suffix1 = parts['0'].Select(x => new double[] { x[1], 2 * x[0] + 1.0 });
                var suffix2 = parts['1'].Select(x => new double[] { x[1], 2 * x[0] - 1.0 });

                // continue recursively on each of the subparts, with new count and prefix
                Histogram(suffix1, Convert.ToInt32((count - delta) / 2.0), prefix + '0');
                Histogram(suffix2, Convert.ToInt32((count + delta) / 2.0), prefix + '1');
            }

            else if (locations.Contains(prefix + '0'))
                Histogram(input.Select(x => new double[] { x[1], 2 * x[0] + 1.0 }), count, prefix + '0');

            else if (locations.Contains(prefix + '1'))
                Histogram(input.Select(x => new double[] { x[1], 2 * x[0] - 1.0 }), count, prefix + '1');

            else for (int i = 0; i < count; i++)
                    WriteRecord(prefix);
        }

        #endregion

        static void Main(string[] args)
        {
            /* Note: various DryadLINQ data sources are commented out, and replaced 
             * with empty data sets to avoid compile errors.
             */
            
            // open DryadLINQ data source.
            //var ddc = new DryadDataContext(@"file://\\sherwood-091\dryadlinqusers\mcsherry");
            //IQueryable<string> searchesdata = ddc.GetPartitionedTable<string>("SearchLogs.txt", CompressionScheme.GZipFast);
            IQueryable<string> searchesdata = Enumerable.Empty<string>().AsQueryable();

            // Encase data sources in PINQueryable privacy type.
            PINQueryable<string> searches = new PINQueryable<string>(searchesdata, new PINQAgentLogger("searches"));

            // extract fields, then restrict to searches for args[0]
            var searchsubset = searches.Select(x => x.Split(','))
                                       .Where(x => x[20].ToLower() == args[0]);

            Console.WriteLine(args[0] + " count: " + searchsubset.NoisyCount(0.1));

            #region Further analysis, and visualization.

            // open second data set, containing ip to latlon mappings.
            //IQueryable<string[]> iplatlondata = ddc.GetPartitionedTable<LineRecord>("IPtoLatLon.txt", CompressionScheme.GZipFast).Select(x => x.line.Split('\t'));
            IQueryable<string[]> iplatlondata = Enumerable.Empty<string>().Select(x => x.Split('\t')).AsQueryable();
            PINQueryable<string[]> iplatlon = new PINQueryable<string[]>(iplatlondata, new PINQAgentLogger("iplatlon"));

            // extract the IP address, and clip off the final octet.
            var searchips = searchsubset.Select(x => x[0].Split('.'))
                                        .Where(x => x.Count() == 4)
                                        .Select(x => x[0] + "." + x[1] + "." + x[2] + ".0");

            // join queries x address; get coords
            var coordinates = from x in searchips
                              join y in iplatlon on x equals y[0]
                              select new double[] { Convert.ToDouble(y.First()[1]) /  90.0, 
                                                    Convert.ToDouble(y.First()[2]) / 180.0 };

            // prepare and output a html page visualization via virtual earth
            WriteHeader(args[0]);               // output the header of the .html
            Histogram(coordinates, 100, "");    // analyze data, output contents
            WriteFooter();                      // output the footer of the .html

            #endregion
        }
    }
}