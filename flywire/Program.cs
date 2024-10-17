using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.IO.Compression;
using System.Text.RegularExpressions;

namespace flywire;

public class FlyBrain {
    public readonly List<Connection> connections = [];
    public readonly Dictionary<long, Cell> cells = new();
    public readonly Dictionary<int, User> users = new();
    public readonly Dictionary<string, Neuropil> neuropils = new();
    public readonly List<Synapse> synapses = [];
    public readonly Dictionary<string, Dictionary<long, Cell>> cellGroups = new();
}

public record struct Point3D(int x, int y, int z);

public class Neuropil(string name) {
    public readonly string name = name;
    public int preCountTotal, preCountProof, postCountTotal, postCountProof;
    public double preProofRatio, postProofRatio;
    public readonly List<Connection> connections = new();

    public override string ToString() => $"Neuropil {name}";
}

public class Label(string label) {
    public readonly string label = label;
    public int labelId;
    public DateTime dateCreated;
    public User user;
    public Point3D position;
    public long supervoxelId;

    public override string ToString() => $"Label {label} {labelId}";
}

public class Cell(long id) {
    public readonly long id = id;
    
    // cell_stats
    public int cableLengthNm;
    public long surfaceAreaNm2, volumeNm3;
    
    // classification
    public string flow, superClass, @class, subClass, cellType, hemibrainType, hemilineage, side, nerve;

    // column_assignment
    public string colHemisphere, colType;
    public int columnId, colX, colY, colP, colQ;
    
    public readonly List<Connection> pre = [], post = [];
    
    // connectivity_tags
    public readonly HashSet<string> connectivityTags = [];
    
    // consolidated_cell_types
    public string primaryType;
    public readonly List<string> additionalTypes = [];
    
    // labels
    public readonly List<Label> labels = [];

    // coordinates
    public Point3D coordPosition;
    public long coordSupervoxelId;
    
    // names
    public string name, group;
    
    // neurons
    public string ntType;
    public double ntTypeScore, daAvg, serAvg, gabaAvg, glutAvg, achAvg, octAvg;

    // neuropil
    public int totalInputSynapses, totalInputPartners, totalOutputSynapses, totalOutputPartners;
    public readonly Dictionary<string, int> inputSynapses = new(), inputPartners = new(),
        outputSynapses = new(), outputPartners = new();
    
    // processed_labels
    public readonly List<string> processedLabels = [];
    
    // visual_neuron_types
    public string visualFamily, visualSubsystem, visualCategory, visualType, visualSide;
    
    public readonly List<Synapse> preSynapses = [], postSynapses = [];
    
    public override string ToString() => $"Cell {id} {name}";
}

public class Synapse(Cell pre, Cell post) {
    public readonly Cell pre = pre, post = post;
    public readonly List<Point3D> coords = [];
    public override string ToString() => $"Synapse {pre} -> {post}";
}

public class Connection(Cell pre, Cell post) {
    public readonly Cell pre = pre, post = post;
    public Neuropil neuropil;
    public int synCount;
    public string ntType;
    public override string ToString() => $"Connection {pre} -> {post} in {neuropil}";
}

public class User(int id) {
    public readonly int id = id;
    public string name, affiliation;
    public override string ToString() => $"User {id} {name}";
}

public partial class Program {
    [GeneratedRegex(@"\s{2,}", RegexOptions.Compiled)]
    private static partial Regex RegexSpaces();

    private static readonly Regex RxSpaces = RegexSpaces();

    private static readonly string Path = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) +
                                          "/dev/notgithub/flywire/";
    
    public static int Main(string[] args) {
        var sw = Stopwatch.StartNew();

        var flyBrain = new FlyBrain();

        ReadCsvGzFile("classification", [
            "root_id", "flow", "super_class", "class", "sub_class",
            "cell_type", "hemibrain_type", "hemilineage", "side", "nerve"
        ], columns => {
            long id = long.Parse(columns[0]);
            flyBrain.cells.Add(id, new Cell(id) {
                flow = columns[1],
                superClass = columns[2],
                @class = columns[3],
                subClass = columns[4],
                cellType = columns[5],
                hemibrainType = columns[6],
                hemilineage = columns[7],
                side = columns[8],
                nerve = columns[9]
            });
            return null;
        });

        ReadCsvGzFile("cell_stats", ["root_id", "length_nm", "area_nm", "size_nm"], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            cell.cableLengthNm = int.Parse(columns[1]);
            cell.surfaceAreaNm2 = long.Parse(columns[2]);
            cell.volumeNm3 = long.Parse(columns[3]);
            return message;
        });

        ReadCsvGzFile("column_assignment", [
            "root_id", "hemisphere", "type", "column_id", "x", "y", "p", "q"
        ], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            cell.colHemisphere = columns[1];
            cell.colType = columns[2];
            cell.columnId = int.Parse(columns[3]);
            cell.colX = int.Parse(columns[4]);
            cell.colY = int.Parse(columns[5]);
            cell.colP = int.Parse(columns[6]);
            cell.colQ = int.Parse(columns[7]);
            return message;
        });
        
        ReadCsvGzFile("connectivity_tags", ["root_id", "connectivity_tag"], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            if (columns[1] != "")
                foreach (var tag in columns[1].Split(','))
                    cell.connectivityTags.Add(tag);
            return message;
        });
        
        ReadCsvGzFile("consolidated_cell_types", [
            "root_id", "primary_type", "additional_type(s)"
        ], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            cell.primaryType = columns[1];
            var additionalTypes = columns[2];
            if (additionalTypes != "")
                cell.additionalTypes.AddRange(additionalTypes.Split(','));
            return message;
        });

        ReadCsvGzFile("coordinates", ["root_id", "position", "supervoxel_id"], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            if (columns[1] != "") {
                string[] xyz = RxSpaces.Replace(columns[1], " ").TrimStart('[').TrimStart(' ').TrimEnd(']').Split(' ');
                cell.coordPosition = new Point3D(int.Parse(xyz[0]), int.Parse(xyz[1]), int.Parse(xyz[2]));
            }
            cell.coordSupervoxelId = long.Parse(columns[2]);
            return message;
        });
        
        ReadCsvGzFile("labels", [
            "root_id", "label", "user_id", "position", "supervoxel_id",
            "label_id", "date_created", "user_name", "user_affiliation"
        ], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            var labelName = columns[1];
            Point3D position = default;
            int userId = int.Parse(columns[2]);
            if (columns[3] != "") {
                string[] xyz = RxSpaces.Replace(columns[3], " ").TrimStart('[').TrimStart(' ').TrimEnd(']').Split(' ');
                position = new Point3D(int.Parse(xyz[0]), int.Parse(xyz[1]), int.Parse(xyz[2]));
            }
            var label = new Label(labelName) {
                position = position,
                supervoxelId = long.Parse(columns[4]),
                labelId = int.Parse(columns[5]),
                dateCreated = DateTime.Parse(columns[6]).ToUniversalTime()
            };
            if (!flyBrain.users.TryGetValue(userId, out var user)) {
                flyBrain.users.Add(userId, user = new User(userId) {
                    name = columns[7],
                    affiliation = columns[8]
                });
            } else {
                Contract.Assert(flyBrain.users[userId].name == columns[7]);
                Contract.Assert(flyBrain.users[userId].affiliation == columns[8]);
            }
            label.user = user;
            cell.labels.Add(label);
            return message;
        });
        
        ReadCsvGzFile("names", ["root_id", "name", "group"], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            cell.name = columns[1];
            cell.group = columns[2];

            if (!flyBrain.cellGroups.TryGetValue(cell.group, out var group))
                flyBrain.cellGroups.Add(cell.group, group = new());
            group.Add(id, cell);
            return message;
        });
        
        ReadCsvGzFile("neurons", [
            "root_id", "group", "nt_type", "nt_type_score", "da_avg",
            "ser_avg", "gaba_avg", "glut_avg", "ach_avg", "oct_avg"
        ], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            string group = columns[1];
            if (cell.group == null) cell.group = group;
            else Contract.Assert(cell.group == group);
            cell.ntType = columns[2];
            cell.ntTypeScore = double.Parse(columns[3]);
            cell.daAvg = double.Parse(columns[4]);
            cell.serAvg = double.Parse(columns[5]);
            cell.gabaAvg = double.Parse(columns[6]);
            cell.glutAvg = double.Parse(columns[7]);
            cell.achAvg = double.Parse(columns[8]);
            cell.octAvg = double.Parse(columns[9]);
            return message;
        });
        
        { // this data is not really needed - can be derived from the connections data
            string[][] neuropilsData = new string[4][];
            // input synapses in    [5..84]  - 79 columns
            // input partners in   [84..163] - 79 columns
            // output synapses in [163..242] - 79 columns
            // output partners in [242..321] - 79 columns

            ReadCsvGzFile("neuropil_synapse_table", header => {
                Contract.Assert(321 == header.Length);
                Contract.Assert(header[0] == "root_id");
                Contract.Assert(header[1] == "input synapses");
                Contract.Assert(header[2] == "input partners");
                Contract.Assert(header[3] == "output synapses");
                Contract.Assert(header[4] == "output partners");
                int skip = 5, take = (header.Length - skip) / 4;
                neuropilsData[0] = header.Skip(skip).Take(take).Select(s => s.Split(' ')[^1]).ToArray();
                neuropilsData[1] = header.Skip(skip += take).Take(take).Select(s => s.Split(' ')[^1]).ToArray();
                neuropilsData[2] = header.Skip(skip += take).Take(take).Select(s => s.Split(' ')[^1]).ToArray();
                neuropilsData[3] = header.Skip(skip + take).Take(take).Select(s => s.Split(' ')[^1]).ToArray();
            }, columns => {
                string message = null;
                long id = long.Parse(columns[0]);
                if (!flyBrain.cells.TryGetValue(id, out var c)) {
                    message = $"missing cell for root_id {id}";
                    flyBrain.cells.Add(id, c = new Cell(id));
                }
                c.totalInputSynapses = int.Parse(columns[1]);
                c.totalInputPartners = int.Parse(columns[2]);
                c.totalOutputSynapses = int.Parse(columns[3]);
                c.totalOutputPartners = int.Parse(columns[4]);
                int col = 5, pil = 0;
                foreach (var d in new[] { c.inputSynapses, c.inputPartners, c.outputSynapses, c.outputPartners }) {
                    foreach (var n in neuropilsData[pil++]) {
                        int v = int.Parse(columns[col++]);
                        if (v != 0) d.Add(n, v);
                    }
                }
                return message;
            });
        }

        ReadCsvGzFile("processed_labels", ["root_id", "processed_labels"], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            var labels = columns[1];
            if (labels != "")
                if (labels.StartsWith("['") && labels.EndsWith("']"))
                    cell.processedLabels.AddRange(labels[2..^2].Split("','"));
                else
                    throw new Exception("processed_labels: unexpected format " + labels);
            return message;
        });

        ReadCsvGzFile("synapse_attachment_rates", [
            "neuropil", "count_total", "count_proof", "proof_ratio", "side"
        ], columns => {
            var name = columns[0];
            if (!flyBrain.neuropils.TryGetValue(name, out var n))
                flyBrain.neuropils[name] = n = new Neuropil(name);
            int countTotal = int.Parse(columns[1]), countProof = int.Parse(columns[2]);
            double proofRatio = double.Parse(columns[3]);
            string side = columns[4];
            if (side == "pre") {
                n.preCountTotal = countTotal;
                n.preCountProof = countProof;
                n.preProofRatio = proofRatio;
            } else if (side == "post") {
                n.postCountTotal = countTotal;
                n.postCountProof = countProof;
                n.postProofRatio = proofRatio;
            } else
                return $"unknown side {side}";
            return null;
        });
        
        // add the neuropils that are missing in the synapse_attachment_rates data
        flyBrain.neuropils.Add("UNASGD", new Neuropil("UNASGD"));

        {
            Cell lastPre = null, lastPost = null;
            Synapse lastSynapse = null;
            ReadCsvGzFile("synapse_coordinates", [
                "pre_root_id", "post_root_id", "x", "y", "z"
            ], columns => {
                string message = null;
                Cell pre = lastPre, post = lastPost;
                string c0 = columns[0], c1 = columns[1];
                if (c0 != "") {
                    long preId = long.Parse(c0);
                    if (!flyBrain.cells.TryGetValue(preId, out pre)) {
                        message = $"missing pre cell for root_id {preId}";
                        flyBrain.cells.Add(preId, pre = new Cell(preId));
                    }
                }
                if (c1 != "") {
                    long postId = long.Parse(c1);
                    if (!flyBrain.cells.TryGetValue(postId, out post)) {
                        message = AppendLine(message, $"missing post cell for root_id {postId}");
                        flyBrain.cells.Add(postId, post = new Cell(postId));
                    }
                }
                Synapse synapse = lastSynapse;
                if (c0 != "" || c1 != "") {
                    synapse = new Synapse(lastPre = pre, lastPost = post);
                    flyBrain.synapses.Add(lastSynapse = synapse);
                    post.preSynapses.Add(synapse);
                    pre.postSynapses.Add(synapse);
                }
                synapse.coords.Add(new Point3D(int.Parse(columns[2]), int.Parse(columns[3]), int.Parse(columns[4])));
                return message;
            });
        }

        ReadCsvGzFile("visual_neuron_types", [
            "root_id", "type", "family", "subsystem", "category", "side"
        ], columns => {
            string message = null;
            long id = long.Parse(columns[0]);
            if (!flyBrain.cells.TryGetValue(id, out var cell)) {
                message = $"missing cell for root_id {id}";
                flyBrain.cells.Add(id, cell = new Cell(id));
            }
            cell.visualType = columns[1];
            cell.visualFamily = columns[2];
            cell.visualSubsystem = columns[3];
            cell.visualCategory = columns[4];
            cell.visualSide = columns[5];
            return message;
        });
        
        ReadCsvGzFile("connections", [ // or "connections_no_threshold"
            "pre_root_id", "post_root_id", "neuropil", "syn_count", "nt_type"
        ], columns => {
            string message = null;
            long preId = long.Parse(columns[0]), postId = long.Parse(columns[1]);
            if (!flyBrain.cells.TryGetValue(preId, out var pre)) {
                message = $"missing pre cell for root_id {preId}";
                flyBrain.cells.Add(preId, pre = new Cell(preId));
            }
            if (!flyBrain.cells.TryGetValue(postId, out var post)) {
                message = AppendLine(message, $"missing post cell for root_id {postId}");
                flyBrain.cells.Add(postId, post = new Cell(postId));
            }
            var neuropilName = columns[2];
            var cn = new Connection(pre, post) {
                neuropil = flyBrain.neuropils.GetValueOrDefault(neuropilName),
                synCount = int.Parse(columns[3]),
                ntType = columns[4]
            };
            if (cn.neuropil == null) {
                message = AppendLine(message, $"missing neuropil {neuropilName}");
                cn.neuropil = new Neuropil(neuropilName);
                flyBrain.neuropils.Add(neuropilName, cn.neuropil);
            }
            cn.pre.post.Add(cn);
            cn.post.pre.Add(cn);
            flyBrain.connections.Add(cn);
            cn.neuropil.connections.Add(cn);
            return message;
        });
        
        Console.WriteLine("done - " + sw.Elapsed);
        return 0;
    }

    private static string AppendLine(string messages, string message) {
        return message == null ? messages : messages == null ? message : messages + Environment.NewLine + message;
    }

    public static void ReadCsvGzFile(string filename, string[] header, Func<string[], string> callback) {
        ReadCsvGzFile(filename, h => {
            Contract.Assert(h.Length == header.Length);
            for (int i = 0; i < h.Length; i++)
                Contract.Assert(h[i] == header[i]);
        }, callback);
    }

    public static void ReadCsvGzFile(string filename, Action<string[]> header, Func<string[], string> callback) {
        Console.Write($"Reading {filename}... ");
        using (var gzip = new GZipStream(File.OpenRead(Path + filename + ".csv.gz"), CompressionMode.Decompress)) {
            using (var reader = new StreamReader(gzip)) {
                string line = reader.ReadLine();
                var h = line.Split(',');
                header(h);
                int recordNo = 0, backspaces = 0;
                long bytes = line.Length + 1;
                while ((line = reader.ReadLine()) != null) {
                    bytes += line.Length + 1;
                    string[] columns = new string[h.Length];
                    int col = 0;
                    for (int i = 0; i >= 0;) {
                        if (line.StartsWith('"')) {
                            i = line.IndexOf("\",");
                            if (i >= 0) {
                                columns[col++] = line[1..i];
                                line = line[(i + 2)..];
                            } else if (line.Length > 1 && line.EndsWith('"')) {
                                columns[col++] = line[1..^1];
                            } else {
                                line += reader.ReadLine();
                                i = 0;
                            }
                        } else {
                            i = line.IndexOf(',');
                            if (i >= 0) {
                                columns[col++] = line[..i];
                                line = line[(i + 1)..];
                            } else {
                                columns[col++] = line;
                            }
                        }
                    }
                    string message = callback(columns);
                    if (message != null) {
                        for (int i = 0; i < backspaces; i++) Console.Write('\b');
                        for (int i = 0; i < backspaces; i++) Console.Write(' ');
                        Console.Write('\r');
                        string[] ss = message.Split(Environment.NewLine);
                        for (int i = 0; i < ss.Length; i++)
                            Console.WriteLine($"{filename}:{recordNo}: {ss[i]}");
                        Console.Write($"Reading {filename}... ");
                        backspaces = 0;
                    }
                    if (++recordNo % (81 * 729 / h.Length) == 0 || message != null) {
                        for (int i = 0; i < backspaces; i++) Console.Write('\b');
                        string s = $"{recordNo:N0} records - {bytes:N0} bytes";
                        Console.Write(s);
                        backspaces = s.Length;
                    }
                }
                for (int i = 0; i < backspaces; i++) Console.Write('\b');
                Console.WriteLine($"{recordNo:N0} records - {bytes:N0} bytes");
            }
        }
    }
}