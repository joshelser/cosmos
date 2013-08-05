package cosmos.accumulo;


/**
 * 
 */
//@Category(IntegrationTests.class)
public class GroupByRowSuffixIteratorTest {
  
//  protected static File tmp = Files.createTempDir();
//  protected static MiniAccumuloCluster mac;
//  protected static MiniAccumuloConfig macConfig;
//  
//  @BeforeClass
//  public static void setup() throws IOException, InterruptedException {
//    macConfig = new MiniAccumuloConfig(tmp, "root");
//    mac = new MiniAccumuloCluster(macConfig);
//    
//    mac.start();
//    
//    // Do this now so we don't forget later (or get an exception)
//    tmp.deleteOnExit();
//  }
//  
//  @AfterClass
//  public static void teardown() throws IOException, InterruptedException {
//    mac.stop();
//  }
//  
//  @Test
//  public void testSingleRow() throws Exception {
//    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
//    Connector c = zk.getConnector("root", new PasswordToken("root"));
//    
//    final String tableName = "foo";
//    
//    if (c.tableOperations().exists(tableName)) {
//      c.tableOperations().delete(tableName);
//    }
//    
//    c.tableOperations().create(tableName);
//    
//    BatchWriter bw = null;
//    try {
//      bw = c.createBatchWriter(tableName, new BatchWriterConfig());
//      
//      Mutation m = new Mutation("1_a");
//      m.put("a", "a", 0, "");
//      m.put("a", "b", 0, "");
//      
//      bw.addMutation(m);
//      
//      m = new Mutation("1_b");
//      m.put("a", "a", 0, "");
//      
//      bw.addMutation(m);
//      
//      m = new Mutation("1_c");
//      m.put("a", "a", 0, "");
//      m.put("b", "a", 0, "");
//      m.put("c", "a", 0, "");
//      
//      bw.addMutation(m);
//    } finally {
//      if (null != bw) {
//        bw.close();
//      }
//    }
//    
//    Map<Key,Long> results = ImmutableMap.<Key,Long> builder().put(new Key("1_a", "a", "b", 0), 2l).put(new Key("1_b", "a", "a", 0), 1l)
//        .put(new Key("1_c", "c", "a", 0), 3l).build();
//    
//    BatchScanner bs = c.createBatchScanner(tableName, new Authorizations(), 1);
//    bs.setRanges(Collections.singleton(new Range()));
//    
//    try {
//      IteratorSetting cfg = new IteratorSetting(30, GroupByRowSuffixIterator.class);
//      
//      bs.addScanIterator(cfg);
//      
//      long count = 0;
//      for (Entry<Key,Value> entry : bs) {
//        VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
//        
//        Assert.assertTrue("Results did not contain: " + entry.getKey(), results.containsKey(entry.getKey()));
//        Assert.assertEquals(results.get(entry.getKey()).longValue(), writable.get());
//        
//        count++;
//      }
//      
//      Assert.assertEquals(results.size(), count);
//    } finally {
//      if (null != bs) {
//        bs.close();
//      }
//    }
//  }
//  
//  @Test
//  public void testMultipleRows() throws Exception {
//    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
//    Connector c = zk.getConnector("root", new PasswordToken("root"));
//    
//    final String tableName = "foo";
//    
//    if (c.tableOperations().exists(tableName)) {
//      c.tableOperations().delete(tableName);
//    }
//    
//    c.tableOperations().create(tableName);
//    
//    BatchWriter bw = null;
//    try {
//      bw = c.createBatchWriter(tableName, new BatchWriterConfig());
//      
//      for (int i = 1; i < 6; i++) {
//        Mutation m = new Mutation(i + "_a");
//        m.put("a", "a", 0, "");
//        m.put("a", "b", 0, "");
//        
//        bw.addMutation(m);
//        
//        m = new Mutation(i + "_b");
//        m.put("a", "a", 0, "");
//        
//        bw.addMutation(m);
//        
//        m = new Mutation(i + "_c");
//        m.put("a", "a", 0, "");
//        m.put("b", "a", 0, "");
//        m.put("c", "a", 0, "");
//        
//        bw.addMutation(m);
//      }
//    } finally {
//      if (null != bw) {
//        bw.close();
//      }
//    }
//    
//    for (int i = 1; i < 6; i++) {
//      Map<Key,Long> results = ImmutableMap.<Key,Long> builder().put(new Key(i + "_a", "a", "b", 0), 2l).put(new Key(i + "_b", "a", "a", 0), 1l)
//          .put(new Key(i + "_c", "c", "a", 0), 3l).build();
//      
//      BatchScanner bs = c.createBatchScanner(tableName, new Authorizations(), 1);
//      bs.setRanges(Collections.singleton(Range.prefix(Integer.toString(i))));
//      
//      try {
//        IteratorSetting cfg = new IteratorSetting(30, GroupByRowSuffixIterator.class);
//        
//        bs.addScanIterator(cfg);
//        
//        long count = 0;
//        for (Entry<Key,Value> entry : bs) {
//          VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
//          
//          Assert.assertTrue("Results did not contain: " + entry.getKey(), results.containsKey(entry.getKey()));
//          Assert.assertEquals(results.get(entry.getKey()).longValue(), writable.get());
//          
//          count++;
//        }
//        
//        Assert.assertEquals(results.size(), count);
//      } finally {
//        if (null != bs) {
//          bs.close();
//        }
//      }
//    }
//  }
//  
//  @Test
//  public void testSingleRowSingleColumn() throws Exception {
//    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
//    Connector c = zk.getConnector("root", new PasswordToken("root"));
//    
//    final String tableName = "foo";
//    
//    if (c.tableOperations().exists(tableName)) {
//      c.tableOperations().delete(tableName);
//    }
//    
//    c.tableOperations().create(tableName);
//    
//    BatchWriter bw = null;
//    try {
//      bw = c.createBatchWriter(tableName, new BatchWriterConfig());
//      
//      Mutation m = new Mutation("1_a");
//      m.put("col1", "1", 0, "");
//      m.put("col1", "2", 0, "");
//      
//      bw.addMutation(m);
//      
//      m = new Mutation("1_b");
//      m.put("col1", "1", 0, "");
//      
//      bw.addMutation(m);
//      
//      m = new Mutation("1_c");
//      m.put("col1", "1", 0, "");
//      m.put("col2", "2", 0, "");
//      m.put("col3", "1", 0, "");
//      
//      bw.addMutation(m);
//    } finally {
//      if (null != bw) {
//        bw.close();
//      }
//    }
//    
//    Map<Key,Long> results = ImmutableMap.<Key,Long> builder().put(new Key("1_a", "col1", "2", 0), 2l).put(new Key("1_b", "col1", "1", 0), 1l)
//        .put(new Key("1_c", "col1", "1", 0), 1l).build();
//    
//    BatchScanner bs = c.createBatchScanner(tableName, new Authorizations(), 1);
//    bs.setRanges(Collections.singleton(new Range()));
//    bs.fetchColumnFamily(new Text("col1"));
//    
//    try {
//      IteratorSetting cfg = new IteratorSetting(30, GroupByRowSuffixIterator.class);
//      
//      bs.addScanIterator(cfg);
//      
//      long count = 0;
//      for (Entry<Key,Value> entry : bs) {
//        VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
//        
//        Assert.assertTrue("Results did not contain: " + entry.getKey(), results.containsKey(entry.getKey()));
//        Assert.assertEquals(results.get(entry.getKey()).longValue(), writable.get());
//        
//        count++;
//      }
//      
//      Assert.assertEquals(results.size(), count);
//    } finally {
//      if (null != bs) {
//        bs.close();
//      }
//    }
//  }
//  
//  @Test
//  public void testManyRowsSingleColumn() throws Exception {
//    ZooKeeperInstance zk = new ZooKeeperInstance(mac.getInstanceName(), mac.getZooKeepers());
//    Connector c = zk.getConnector("root", new PasswordToken("root"));
//    
//    final String tableName = "foo";
//    
//    if (c.tableOperations().exists(tableName)) {
//      c.tableOperations().delete(tableName);
//    }
//    
//    c.tableOperations().create(tableName);
//    
//    BatchWriter bw = null;
//    try {
//      bw = c.createBatchWriter(tableName, new BatchWriterConfig());
//      
//      for (int i = 1; i < 6; i++) {
//        Mutation m = new Mutation(i + "_a");
//        m.put("col1", "1", 0, "");
//        m.put("col1", "2", 0, "");
//        
//        bw.addMutation(m);
//        
//        m = new Mutation(i + "_b");
//        m.put("col1", "1", 0, "");
//        
//        bw.addMutation(m);
//        
//        m = new Mutation(i + "_c");
//        m.put("col1", "1", 0, "");
//        m.put("col2", "2", 0, "");
//        m.put("col3", "1", 0, "");
//        
//        bw.addMutation(m);
//      }
//    } finally {
//      if (null != bw) {
//        bw.close();
//      }
//    }
//    
//    for (int i = 1; i < 6; i++) {
//      Map<Key,Long> results = ImmutableMap.<Key,Long> builder().put(new Key(i + "_a", "col1", "2", 0), 2l).put(new Key(i + "_b", "col1", "1", 0), 1l)
//          .put(new Key(i + "_c", "col1", "1", 0), 1l).build();
//      
//      BatchScanner bs = c.createBatchScanner(tableName, new Authorizations(), 1);
//      bs.setRanges(Collections.singleton(Range.prefix(Integer.toString(i))));
//      bs.fetchColumnFamily(new Text("col1"));
//      
//      try {
//        IteratorSetting cfg = new IteratorSetting(30, GroupByRowSuffixIterator.class);
//        
//        bs.addScanIterator(cfg);
//        
//        long count = 0;
//        for (Entry<Key,Value> entry : bs) {
//          VLongWritable writable = GroupByRowSuffixIterator.getWritable(entry.getValue());
//          
//          Assert.assertTrue("Results did not contain: " + entry.getKey(), results.containsKey(entry.getKey()));
//          Assert.assertEquals(results.get(entry.getKey()).longValue(), writable.get());
//          
//          count++;
//        }
//        
//        Assert.assertEquals(results.size(), count);
//      } finally {
//        if (null != bs) {
//          bs.close();
//        }
//      }
//    }
//  }
//  
//  @Test
//  public void testReseek() throws Exception {
//    TreeMap<Key,Value> data = Maps.newTreeMap();
//    data.put(new Key("foo\u0000bell", "RESTAURANT", "f\u00001"), new Value());
//    data.put(new Key("foo\u0000bell", "RESTAURANT", "f\u00002"), new Value());
//    data.put(new Key("foo\u0000taco", "RESTAURANT", "f\u00001"), new Value());
//    data.put(new Key("foo\u0000taco", "RESTAURANT", "f\u00002"), new Value());
//    data.put(new Key("foo\u0000taco", "RESTAURANT", "f\u00003"), new Value());
//    
//    SortedMapIterator source = new SortedMapIterator(data);
//    
//    GroupByRowSuffixIterator iter = new GroupByRowSuffixIterator();
//    iter.init(source, Collections.<String,String> emptyMap(), new IteratorEnvironment() {
//      @Override
//      public SortedKeyValueIterator<Key,Value> reserveMapFileReader(final String mapFileName) throws IOException {
//        return null;
//      }
//      
//      @Override
//      public AccumuloConfiguration getConfig() {
//        return null;
//      }
//      
//      @Override
//      public IteratorScope getIteratorScope() {
//        return null;
//      }
//      
//      @Override
//      public boolean isFullMajorCompaction() {
//        return false;
//      }
//      
//      @Override
//      public void registerSideChannel(final SortedKeyValueIterator<Key,Value> iter) {}
//    });
//    
//    iter.seek(Range.prefix("foo"), Collections.<ByteSequence> emptySet(), false);
//    
//    Assert.assertTrue(iter.hasTop());
//    
//    Assert.assertEquals(new Key("foo\u0000bell", "RESTAURANT", "f\u00002"), iter.getTopKey());
//    VLongWritable actual = new VLongWritable(), expected = new VLongWritable(2);
//    
//    actual.readFields(new DataInputStream(new ByteArrayInputStream(iter.getTopValue().get())));
//    Assert.assertEquals(expected, actual);
//    
//    iter.seek(new Range(new Key("foo\u0000bell", "RESTAURANT", "f\u00002"), false, new Key("fop"), false), Collections.<ByteSequence> emptySet(), false);
//    
//    Assert.assertTrue(iter.hasTop());
//
//    Assert.assertEquals(new Key("foo\u0000taco", "RESTAURANT", "f\u00003"), iter.getTopKey());
//    actual = new VLongWritable();
//    expected = new VLongWritable(3);
//    
//    actual.readFields(new DataInputStream(new ByteArrayInputStream(iter.getTopValue().get())));
//    Assert.assertEquals(expected, actual);
//    
//    iter.next();
//    
//    Assert.assertFalse(iter.hasTop());
//  }
}
