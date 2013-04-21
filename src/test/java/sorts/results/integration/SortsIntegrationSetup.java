package sorts.results.integration;

import java.io.InputStream;
import java.util.List;
import java.util.zip.GZIPInputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Unmarshaller;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.mediawiki.xml.export_0.MediaWikiType;
import org.mediawiki.xml.export_0.PageType;

import sorts.results.Column;
import sorts.results.SValue;
import sorts.results.impl.MultimapQueryResult;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;


/**
 * 
 */
@Category(IntegrationTests.class)
public class SortsIntegrationSetup {
  public static final String PAGE_ID = "PAGE_ID", PAGE_TITLE = "PAGE_TITLE", PAGE_RESTRICTIONS = "PAGE_RESTRICTIONS";
      
  public static final String ARTICLE_BASE = "/enwiki-20111201-metadata-articles-", ARTICLE_SUFFIX = ".xml.gz";
  
  private static final Cache<String,MediaWikiType> wikiCache = CacheBuilder.newBuilder().concurrencyLevel(5).build();
  
  private static final String WIKI1 = "wiki1", WIKI2 = "wiki2", WIKI3 = "wiki3", WIKI4 = "wiki4", WIKI5 = "wiki5";
  private static JAXBContext context;
  
  @BeforeClass
  public static void initializeJaxb() throws Exception {
    context = JAXBContext.newInstance("org.mediawiki.xml.export_0", ClassLoader.getSystemClassLoader());
  }
  
  public static void clearCache() {
    wikiCache.invalidateAll();
  }
  
  public static void loadAllWikis() throws Exception {
    List<Thread> threads = Lists.newArrayList();
    
    threads.add(new Thread(new Runnable() {
      public void run() {
        try {
          SortsIntegrationSetup.getWiki1();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }));
    
    threads.add(new Thread(new Runnable() {
      public void run() {
        try {
          SortsIntegrationSetup.getWiki2();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }));
    
    threads.add(new Thread(new Runnable() {
      public void run() {
        try {
          SortsIntegrationSetup.getWiki3();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }));
    
    threads.add(new Thread(new Runnable() {
      public void run() {
        try {
          SortsIntegrationSetup.getWiki4();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }));
    
    threads.add(new Thread(new Runnable() {
      public void run() {
        try {
          SortsIntegrationSetup.getWiki5();
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    }));
    
    for (Thread t : threads) {
      t.start();
    }
    
    for (Thread t : threads) {
      t.join();
    }
  }
  
  public static MediaWikiType getWiki1() throws Exception {
    MediaWikiType wiki1 = wikiCache.getIfPresent(WIKI1);
    if (null == wiki1) {
      synchronized (WIKI1) {
        wiki1 = wikiCache.getIfPresent(WIKI1);
        if (null == wiki1) {
          wiki1 = loadWiki(1);
          wikiCache.put(WIKI1, wiki1);
        }
      }
    }
    
    return wiki1;
  }
  
  public static MediaWikiType getWiki2() throws Exception {
    MediaWikiType wiki2 = wikiCache.getIfPresent(WIKI2);
    if (null == wiki2) {
      synchronized (WIKI2) {
        wiki2 = wikiCache.getIfPresent(WIKI2);
        if (null == wiki2) {
          wiki2 = loadWiki(2);
          wikiCache.put(WIKI2, wiki2);
        }
      }
    }
    
    return wiki2;
  }
  
  public static MediaWikiType getWiki3() throws Exception {
    MediaWikiType wiki3 = wikiCache.getIfPresent(WIKI3);
    if (null == wiki3) {
      synchronized (WIKI3) {
        wiki3 = wikiCache.getIfPresent(WIKI3);
        if (null == wiki3) {
          wiki3 = loadWiki(3);
          wikiCache.put(WIKI3, wiki3);
        }
      }
    }
    
    return wiki3;
  }
  
  public static MediaWikiType getWiki4() throws Exception {
    MediaWikiType wiki4 = wikiCache.getIfPresent(WIKI4);
    if (null == wiki4) {
      synchronized (WIKI4) {
        wiki4 = wikiCache.getIfPresent(WIKI4);
        if (null == wiki4) {
          wiki4 = loadWiki(4);
          wikiCache.put(WIKI4, wiki4);
        }
      }
    }
    
    return wiki4;
  }
  
  public static MediaWikiType getWiki5() throws Exception {
    MediaWikiType wiki5 = wikiCache.getIfPresent(WIKI5);
    if (null == wiki5) {
      synchronized (WIKI5) {
        wiki5 = wikiCache.getIfPresent(WIKI5);
        if (null == wiki5) {
          wiki5 = loadWiki(5);
          wikiCache.put(WIKI5, wiki5);
        }
      }
    }
    
    return wiki5;
  }
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  protected static MediaWikiType loadWiki(int num) throws Exception {
    Unmarshaller unmarshaller = context.createUnmarshaller();
    
    InputStream is = SortsIntegrationSetup.class.getResourceAsStream(ARTICLE_BASE + num + ARTICLE_SUFFIX);
    
    Assert.assertNotNull(is);
    
    GZIPInputStream gzip = new GZIPInputStream(is);
    Object o = unmarshaller.unmarshal(gzip);
    
    Assert.assertEquals(JAXBElement.class, o.getClass());
    Assert.assertEquals(MediaWikiType.class, ((JAXBElement) o).getDeclaredType());
    
    JAXBElement<MediaWikiType> jaxb = (JAXBElement<MediaWikiType>) o;
    
    return jaxb.getValue();
  }
  
  public static List<MultimapQueryResult> wikiToMultimap(MediaWikiType wiki) {
    Preconditions.checkNotNull(wiki);
    
    List<PageType> pages = wiki.getPage();
    List<MultimapQueryResult> mmap = Lists.newArrayList();
    final String lang = wiki.getLang();
    final ColumnVisibility viz = new ColumnVisibility(lang); 
    long id = 0l;
    
    for (PageType page : pages) {
      Multimap<Column,SValue> data = HashMultimap.create();
      
      data.put(Column.create(PAGE_ID), SValue.create(page.getId().toString(), viz));
      data.put(Column.create(PAGE_TITLE), SValue.create(page.getTitle(), viz));
      
      if (!StringUtils.isBlank(page.getRestrictions())) {
        data.put(Column.create(PAGE_RESTRICTIONS), SValue.create(page.getRestrictions(), viz));
      }
      
      mmap.add(new MultimapQueryResult(data, lang + id, viz));
    }
    
    
    return mmap;
  }
  
}