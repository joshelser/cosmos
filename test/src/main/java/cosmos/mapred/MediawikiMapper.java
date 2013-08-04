package cosmos.mapred;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.mediawiki.xml.export_0.ContributorType;
import org.mediawiki.xml.export_0.MediaWikiType;
import org.mediawiki.xml.export_0.PageType;
import org.mediawiki.xml.export_0.RevisionType;


import com.sun.xml.bind.api.ClassResolver;

import cosmos.mediawiki.MediawikiPage.Page;
import cosmos.mediawiki.MediawikiPage.Page.Revision;
import cosmos.mediawiki.MediawikiPage.Page.Revision.Contributor;

/**
 * 
 */
public class MediawikiMapper extends Mapper<LongWritable,Text,Text,Mutation> {
  private static final Text tableName = new Text("sortswiki");
  private static final Text empty = new Text("");
  
  private static Object lock = new Object();
  private static JAXBContext jaxbCtx;
  
  private Unmarshaller unmarshaller;

  public MediawikiMapper() {
    synchronized(lock) {
      try {
        jaxbCtx = JAXBContext.newInstance("org.mediawiki.xml.export_0", ClassLoader.getSystemClassLoader());
      } catch (JAXBException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  /**
   * Called once at the beginning of the task.
   */
  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    try {
      this.unmarshaller = jaxbCtx.createUnmarshaller();
      this.unmarshaller.setProperty(ClassResolver.class.getName(), new ClassResolver() {

        @Override
        public Class<?> resolveElementName(String nsUri, String localName) throws Exception {
          if ("page".equalsIgnoreCase(localName)) {
            return PageType.class;
          } else {
            return MediaWikiType.class;
          }
        }
        
      });
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Called once for each key/value pair in the input split. Most applications should override this, but the default is the identity function.
   */
  @Override
  protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    Object o;
    try {
      o = unmarshaller.unmarshal(new ByteArrayInputStream(value.getBytes(), 0, value.getLength()));
    } catch (JAXBException e) {
      throw new IOException("Couldn't unmarshall '" + value + "'", e);
    }
    
    
    PageType pageType = (PageType) o;
    
    Page page = pageTypeToPage(pageType);
    
    Value protobufValue = new Value(page.toByteArray());
    
    Mutation m = new Mutation(Long.toString(page.getId()));
    m.put(empty, empty, protobufValue);
    
    context.write(tableName, m);
  }
  
  protected Page pageTypeToPage(PageType pageType) {
    Page.Builder builder = Page.newBuilder();
    
    builder.setId(pageType.getId().longValue());
    
    List<Object> children = pageType.getRevisionOrUploadOrLogitem();
    if (null != children) {
      for (Object o : children) {
        if (o instanceof RevisionType) {
          RevisionType revision = (RevisionType) o;
          
          Revision.Builder revBuilder = Revision.newBuilder();
          
          revBuilder.setId(revision.getId().longValue());
          revBuilder.setTimestamp(revision.getTimestamp().toString());
          
          ContributorType contributor = revision.getContributor();
          
          if (contributor != null) {
            Contributor.Builder conBuilder = Contributor.newBuilder();
            if (null != contributor.getId()) {
              conBuilder.setId(contributor.getId().longValue());
            }
            if (null != contributor.getUsername()) {
              conBuilder.setUsername(contributor.getUsername());
            }
            
            revBuilder.setContributor(conBuilder.build());
          }
          
          builder.setRevision(revBuilder.build());
          
          break;
        }
      }
    }
    
    return builder.build();
  }
  
  /**
   * Called once at the end of the task.
   */
  @Override
  protected void cleanup(Context context) throws IOException, InterruptedException {
  }
}
