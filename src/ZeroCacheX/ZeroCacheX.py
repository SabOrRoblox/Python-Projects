from __future__ import annotations
import time
import sys
from enum import Enum
from typing import Any, Callable, TypeVar, ParamSpec, Optional
from collections import OrderedDict, defaultdict
from dataclasses import dataclass
from functools import _make_key as make_key, wraps
import threading
import heapq
from concurrent.futures import ThreadPoolExecutor, as_completed
import asyncio

T = TypeVar('T')
P = ParamSpec('P')

class CacheStrategy(Enum):
    LRU = "lru"
    MRU = "mru"
    TTL = "ttl"
    LFU = "lfu"

@dataclass(frozen=True)
class CacheInfo:
    hits: int
    misses: int
    maxsize: int
    currsize: int
    strategy: CacheStrategy
    memory_used: int
    avg_response_time: float
    total_operations: int
    ttl: Optional[float] = None

class CacheEntry:
    __slots__ = ("value", "timestamp", "access_count", "expires_at", "size")

    def __init__(self, value: Any, ttl: Optional[float] = None, size_func: Optional[Callable[[Any], int]] = None):  
        self.value = value  
        self.timestamp = time.time()  
        self.access_count = 0  
        self.expires_at = self.timestamp + ttl if ttl is not None else None  
          
        if size_func:  
            self.size = size_func(value)  
        else:  
            try:  
                self.size = max(64, sys.getsizeof(value))  
            except:  
                self.size = 128  

    def is_expired(self) -> bool:  
        return self.expires_at is not None and time.time() > self.expires_at  

    def touch(self):  
        self.access_count += 1

class CacheBase:
    __slots__ = ('_cache', '_hits', '_misses', '_maxsize', '_strategy', '_lock',
                 '_current_size', '_total_response_time', '_total_operations', '_memory_limit',
                 '_sample_counter', '_sample_rate')

    def __init__(self, maxsize: Optional[int] = 128, strategy: CacheStrategy = CacheStrategy.LRU,  
                 memory_limit: Optional[int] = None):  
        self._lock = threading.Lock()  
        self._cache = {}  
        self._hits = 0  
        self._misses = 0  
        self._maxsize = maxsize  
        self._strategy = strategy  
        self._current_size = 0  
        self._total_response_time = 0.0  
        self._total_operations = 0  
        self._memory_limit = memory_limit  
        self._sample_counter = 0  
        self._sample_rate = 100  

    def _record_time(self, start_time: float) -> None:  
        self._sample_counter += 1  
        if self._sample_counter >= self._sample_rate:  
            end_time = time.perf_counter()  
            self._total_response_time += (end_time - start_time) * self._sample_rate  
            self._total_operations += self._sample_rate  
            self._sample_counter = 0  

    def get(self, key: Any) -> Any:  
        start_time = time.perf_counter() if self._sample_counter == 0 else 0  
          
        with self._lock:  
            if key in self._cache:  
                entry = self._cache[key]  
                if entry.is_expired():  
                    self._remove_key(key)  
                    self._misses += 1  
                else:  
                    self._update_access(key, entry)  
                    self._hits += 1  
                    self._record_time(start_time)  
                    return entry.value  
            else:  
                self._misses += 1  
              
            self._record_time(start_time)  
            return None  

    def set(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:  
        start_time = time.perf_counter() if self._sample_counter == 0 else 0  
          
        with self._lock:  
            if key in self._cache:  
                self._remove_key(key)  
              
            entry = CacheEntry(value, ttl, self._get_size_func())  
            entry_size = entry.size  
              
            if self._maxsize is not None and self._maxsize <= 0:  
                pass  
            else:  
                self._cache[key] = entry  
                self._current_size += entry_size  
                self._update_access(key, entry)  
                self._evict_if_needed()  
              
            self._record_time(start_time)  

    def _get_size_func(self) -> Optional[Callable[[Any], int]]:  
        return None  

    def _remove_key(self, key: Any) -> None:  
        if key in self._cache:  
            entry = self._cache[key]  
            del self._cache[key]  
            self._current_size -= entry.size  

    def _update_access(self, key: Any, entry: CacheEntry) -> None:  
        pass  

    def _evict_if_needed(self) -> None:  
        pass  

    def _should_evict(self) -> bool:  
        if self._maxsize is not None and len(self._cache) > self._maxsize:  
            return True  
        if self._memory_limit is not None and self._current_size > self._memory_limit:  
            return True  
        return False  

    def delete(self, key: Any) -> bool:  
        start_time = time.perf_counter() if self._sample_counter == 0 else 0  
          
        with self._lock:  
            if key in self._cache:  
                self._remove_key(key)  
                self._record_time(start_time)  
                return True  
              
            self._record_time(start_time)  
            return False  

    def clear(self) -> None:  
        with self._lock:  
            self._cache.clear()  
            self._hits = 0  
            self._misses = 0  
            self._current_size = 0  
            self._total_response_time = 0.0  
            self._total_operations = 0  
            self._sample_counter = 0  

    def info(self) -> CacheInfo:  
        with self._lock:  
            if self._total_operations > 0:  
                avg_response_time = self._total_response_time / self._total_operations  
            else:  
                avg_response_time = 0  
                  
            return CacheInfo(  
                hits=self._hits,  
                misses=self._misses,  
                maxsize=self._maxsize or 0,  
                currsize=len(self._cache),  
                strategy=self._strategy,  
                memory_used=self._current_size,  
                avg_response_time=avg_response_time,  
                total_operations=self._total_operations,  
                ttl=None  
            )

class OptimizedLFUCache(CacheBase):
    __slots__ = ('_freq_nodes', '_min_freq', '_freq_dict')

    def __init__(self, maxsize: Optional[int] = 128, memory_limit: Optional[int] = None):  
        super().__init__(maxsize, CacheStrategy.LFU, memory_limit)  
        self._freq_nodes = defaultdict(OrderedDict)  
        self._min_freq = 0  
        self._freq_dict = {}  

    def _remove_key(self, key: Any) -> None:  
        if key in self._cache:  
            old_entry = self._cache[key]  
            old_freq = self._freq_dict[key]  
              
            del self._freq_nodes[old_freq][key]  
            if not self._freq_nodes[old_freq]:  
                del self._freq_nodes[old_freq]  
                if old_freq == self._min_freq:  
                    self._min_freq = old_freq + 1  
              
            del self._freq_dict[key]  
            super()._remove_key(key)  

    def _update_access(self, key: Any, entry: CacheEntry) -> None:  
        entry.touch()  
        if key in self._freq_dict:  
            old_freq = self._freq_dict[key]  
            new_freq = old_freq + 1  
              
            del self._freq_nodes[old_freq][key]  
            if not self._freq_nodes[old_freq]:  
                del self._freq_nodes[old_freq]  
                if old_freq == self._min_freq:  
                    self._min_freq = new_freq  
              
            self._freq_nodes[new_freq][key] = entry  
            self._freq_dict[key] = new_freq  
        else:  
            self._freq_nodes[1][key] = entry  
            self._freq_dict[key] = 1  
            self._min_freq = 1  

    def _evict_if_needed(self) -> None:  
        while self._should_evict() and self._freq_nodes:  
            while self._min_freq not in self._freq_nodes and self._freq_nodes:  
                self._min_freq = min(self._freq_nodes.keys())  
              
            if not self._freq_nodes.get(self._min_freq):  
                continue  
              
            key, _ = self._freq_nodes[self._min_freq].popitem(last=False)  
            if not self._freq_nodes[self._min_freq]:  
                del self._freq_nodes[self._min_freq]  
              
            if key in self._freq_dict:  
                del self._freq_dict[key]  
            if key in self._cache:  
                super()._remove_key(key)

class TTLCache(CacheBase):
    __slots__ = ('_default_ttl', '_expiry_heap', '_cleanup_thread', '_cleanup_interval', '_running')

    def __init__(self, maxsize: Optional[int] = 128, default_ttl: Optional[float] = None,   
                 memory_limit: Optional[int] = None, cleanup_interval: float = 60.0):  
        super().__init__(maxsize, CacheStrategy.TTL, memory_limit)  
        self._default_ttl = default_ttl  
        self._expiry_heap = []  
        self._cleanup_interval = cleanup_interval  
        self._running = True  
          
        self._cleanup_thread = threading.Thread(target=self._background_cleanup, daemon=True)  
        self._cleanup_thread.start()  
      
    def _background_cleanup(self) -> None:  
        while self._running:  
            with self._lock:  
                if self._expiry_heap:  
                    next_expire = self._expiry_heap[0][0]  
                    timeout = max(0, next_expire - time.time())  
                else:  
                    timeout = self._cleanup_interval  
              
            time.sleep(min(timeout, self._cleanup_interval))  
            self.cleanup_expired()  
      
    def cleanup_expired(self) -> int:  
        with self._lock:  
            now = time.time()  
            expired_count = 0  
              
            while self._expiry_heap and self._expiry_heap[0][0] <= now:  
                expires_at, key = heapq.heappop(self._expiry_heap)  
                if key in self._cache and self._cache[key].expires_at == expires_at:  
                    if self._cache[key].is_expired():  
                        super()._remove_key(key)  
                        expired_count += 1  
                  
                if expired_count >= 1000:  
                    break  
              
            return expired_count  
      
    def set(self, key: Any, value: Any, ttl: Optional[float] = None) -> None:  
        start_time = time.perf_counter() if self._sample_counter == 0 else 0  
          
        with self._lock:  
            if key in self._cache:  
                super()._remove_key(key)  
              
            actual_ttl = ttl if ttl is not None else self._default_ttl  
            entry = CacheEntry(value, actual_ttl, self._get_size_func())  
            entry_size = entry.size  
              
            if self._maxsize is not None and self._maxsize <= 0:  
                pass  
            else:  
                self._cache[key] = entry  
                self._current_size += entry_size  
                  
                if actual_ttl is not None:  
                    heapq.heappush(self._expiry_heap, (entry.expires_at, key))  
                  
                self._evict_if_needed()  
              
            self._record_time(start_time)  
      
    def _evict_if_needed(self) -> None:  
        while self._should_evict() and self._cache:  
            oldest_key = None  
            oldest_time = float('inf')  
              
            for key, entry in self._cache.items():  
                if entry.timestamp < oldest_time:  
                    oldest_time = entry.timestamp  
                    oldest_key = key  
              
            if oldest_key is not None:  
                super()._remove_key(oldest_key)  
      
    def close(self) -> None:  
        self._running = False  
        if self._cleanup_thread.is_alive():  
            self._cleanup_thread.join(timeout=1.0)  
      
    def info(self) -> CacheInfo:
        with self._lock:
            base_info = super().info()
            base_dict = base_info.__dict__.copy()
            base_dict['ttl'] = self._default_ttl  # заменяем ttl
            return CacheInfo(**base_dict)

class HighPerformanceCache(CacheBase):
    __slots__ = ('_order',)

    def __init__(self, maxsize: Optional[int] = 128, strategy: CacheStrategy = CacheStrategy.LRU,   
                 memory_limit: Optional[int] = None):  
        super().__init__(maxsize, strategy, memory_limit)  
        self._order = OrderedDict()  
      
    def _update_access(self, key: Any, entry: CacheEntry) -> None:  
        if self._strategy == CacheStrategy.LRU:  
            if key in self._order:  
                self._order.move_to_end(key)  
            else:  
                self._order[key] = True  
        elif self._strategy == CacheStrategy.MRU:  
            if key in self._order:  
                self._order.move_to_end(key, last=False)  
            else:  
                self._order[key] = True  
      
    def _remove_key(self, key: Any) -> None:  
        if key in self._order:  
            del self._order[key]  
        super()._remove_key(key)  
      
    def _evict_if_needed(self) -> None:  
        while self._should_evict() and self._order:  
            if self._strategy == CacheStrategy.LRU:  
                key, _ = self._order.popitem(last=False)  
            elif self._strategy == CacheStrategy.MRU:  
                key, _ = self._order.popitem(last=True)  
              
            if key in self._cache:  
                super()._remove_key(key)  
      
    def clear(self) -> None:  
        with self._lock:  
            self._order.clear()  
            super().clear()

_cache_registry = set()
_registry_lock = threading.Lock()
_local_executor = ThreadPoolExecutor(max_workers=16, thread_name_prefix="cache_batch")
_uloop = None
_uloop_lock = threading.Lock()

def _get_uloop() -> asyncio.AbstractEventLoop:
    global _uloop
    with _uloop_lock:
        if _uloop is None:
            try:
                _uloop = asyncio.get_running_loop()
            except RuntimeError:
                _uloop = asyncio.new_event_loop()
                asyncio.set_event_loop(_uloop)
        return _uloop

async def async_batch_set_operations(cache_instance: HighPerformanceCache, key_value_pairs: list, ttl: Optional[float] = None) -> list:
    def _batch_set_chunk(chunk):
        results = []
        for key, value in chunk:
            cache_instance.set(key, value, ttl)
            results.append(True)
        return results

    chunk_size = max(10, len(key_value_pairs) // 16)  
    chunks = [key_value_pairs[i:i + chunk_size] for i in range(0, len(key_value_pairs), chunk_size)]  
    
    loop = _get_uloop()  
    
    with ThreadPoolExecutor(max_workers=min(16, len(chunks))) as executor:  
        futures = [  
            loop.run_in_executor(executor, _batch_set_chunk, chunk)  
            for chunk in chunks  
        ]  
        results = []  
        for future in asyncio.as_completed(futures):  
            chunk_results = await future  
            results.extend(chunk_results)  
    
    return results

async def async_batch_get_operations(cache_instance: HighPerformanceCache, keys: list) -> list:
    def _batch_get_chunk(chunk):
        results = []
        for key in chunk:
            results.append(cache_instance.get(key))
        return results

    chunk_size = max(10, len(keys) // 16)  
    chunks = [keys[i:i + chunk_size] for i in range(0, len(keys), chunk_size)]  
    
    loop = _get_uloop()  
    
    with ThreadPoolExecutor(max_workers=min(16, len(chunks))) as executor:  
        futures = [  
            loop.run_in_executor(executor, _batch_get_chunk, chunk)  
            for chunk in chunks  
        ]  
        results = []  
        for future in asyncio.as_completed(futures):  
            chunk_results = await future  
            results.extend(chunk_results)  
    
    return results

def batch_set_operations(cache_instance: HighPerformanceCache, key_value_pairs: list, ttl: Optional[float] = None) -> list:
    def _batch_set_chunk(chunk):
        results = []
        for key, value in chunk:
            cache_instance.set(key, value, ttl)
            results.append(True)
        return results

    chunk_size = max(10, len(key_value_pairs) // 16)  
    chunks = [key_value_pairs[i:i + chunk_size] for i in range(0, len(key_value_pairs), chunk_size)]  
    
    with ThreadPoolExecutor(max_workers=min(16, len(chunks))) as executor:  
        futures = [executor.submit(_batch_set_chunk, chunk) for chunk in chunks]  
        results = []  
        for future in as_completed(futures):  
            results.extend(future.result())  
    
    return results

def batch_get_operations(cache_instance: HighPerformanceCache, keys: list) -> list:
    def _batch_get_chunk(chunk):
        results = []
        for key in chunk:
            results.append(cache_instance.get(key))
        return results

    chunk_size = max(10, len(keys) // 16)  
    chunks = [keys[i:i + chunk_size] for i in range(0, len(keys), chunk_size)]  
    
    with ThreadPoolExecutor(max_workers=min(16, len(chunks))) as executor:  
        futures = [executor.submit(_batch_get_chunk, chunk) for chunk in chunks]  
        results = []  
        for future in as_completed(futures):  
            results.extend(future.result())  
    
    return results

def cache(
    maxsize: Optional[int] = 128,
    ttl: Optional[float] = None,
    strategy: CacheStrategy | str = CacheStrategy.LRU,
    typed: bool = False,
    memory_limit: Optional[int] = None
):
    strategy_map = {
        "lru": CacheStrategy.LRU,
        "lfu": CacheStrategy.LFU,
        "mru": CacheStrategy.MRU,
        "ttl": CacheStrategy.TTL,
    }

    if isinstance(strategy, str):  
        cache_strategy = strategy_map.get(strategy.lower(), CacheStrategy.LRU)  
    else:  
        cache_strategy = strategy  
    
    def decorator(user_function: Callable[P, T]) -> Callable[P, T]:  
        if cache_strategy == CacheStrategy.TTL or ttl is not None:  
            cache_instance = TTLCache(maxsize, ttl, memory_limit)  
        elif cache_strategy == CacheStrategy.LFU:  
            cache_instance = OptimizedLFUCache(maxsize, memory_limit)  
        else:  
            cache_instance = HighPerformanceCache(maxsize, cache_strategy, memory_limit)  
          
        with _registry_lock:  
            _cache_registry.add(cache_instance)  
          
        @wraps(user_function)  
        def sync_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:  
            key = make_key(args, kwargs, typed)  
              
            cached_result = cache_instance.get(key)  
            if cached_result is not None:  
                return cached_result  
              
            result = user_function(*args, **kwargs)  
            cache_instance.set(key, result, ttl)  
            return result  
          
        @wraps(user_function)  
        async def async_wrapper(*args: P.args, **kwargs: P.kwargs) -> T:  
            key = make_key(args, kwargs, typed)  
              
            cached_result = cache_instance.get(key)  
            if cached_result is not None:  
                return cached_result  
              
            result = user_function(*args, **kwargs)  
            if asyncio.iscoroutine(result):  
                result = await result  
              
            cache_instance.set(key, result, ttl)  
            return result  
          
        wrapper = async_wrapper if asyncio.iscoroutinefunction(user_function) else sync_wrapper  
          
        wrapper.cache_clear = cache_instance.clear  
        wrapper.cache_info = cache_instance.info  
          
        def cache_delete_wrapper(*args: P.args, **kwargs: P.kwargs) -> bool:  
            key_to_delete = make_key(args, kwargs, typed)  
            return cache_instance.delete(key_to_delete)  
          
        wrapper.cache_delete = cache_delete_wrapper  
          
        if isinstance(cache_instance, TTLCache):  
            wrapper.cleanup_expired = cache_instance.cleanup_expired  
            wrapper.close = cache_instance.close  
          
        return wrapper  
    
    return decorator

class cached_property:
    __slots__ = ('_ttl', '_func', '_attrname', '_lock', '__doc__')

    def __init__(self, ttl: Optional[float] = None):  
        self._ttl = ttl  
        self._func = None  
        self._attrname = None  
        self._lock = threading.RLock()  
        self.__doc__ = None
  
    def __call__(self, func: Callable) -> 'cached_property':  
        self._func = func  
        self.__doc__ = func.__doc__  
        return self  
      
    def __set_name__(self, owner, name):  
        if self._attrname is None:  
            self._attrname = name  
        elif name != self._attrname:  
            raise TypeError("Cannot assign cached_property to different names")  
      
    def __get__(self, instance, owner=None):  
        if instance is None:  
            return self  
          
        cache = instance.__dict__  
        cache_key = f"_{self._attrname}_cached"  
        cache_time_key = f"_{self._attrname}_timestamp"  
          
        now = time.time()  
          
        with self._lock:  
            cached_value = cache.get(cache_key)  
            cached_time = cache.get(cache_time_key, 0)  
              
            is_valid = (cached_value is not None and   
                        (self._ttl is None or now - cached_time < self._ttl))  
              
            if is_valid:  
                return cached_value  
              
            result = self._func(instance)  
              
            cache[cache_key] = result  
            cache[cache_time_key] = now  
            return result  
      
    def expire(self, instance) -> bool:  
        cache_key = f"_{self._attrname}_cached"  
        cache_time_key = f"_{self._attrname}_timestamp"  
          
        with self._lock:  
            deleted = False  
            if cache_key in instance.__dict__:  
                del instance.__dict__[cache_key]  
                deleted = True  
            if cache_time_key in instance.__dict__:  
                del instance.__dict__[cache_time_key]  
            return deleted

class AsyncCachedProperty:
    __slots__ = ('_ttl', '_func', '_attrname', '_lock', '__doc__')

    def __init__(self, ttl: Optional[float] = None):  
        self._ttl = ttl  
        self._func = None  
        self._attrname = None  
        self._lock = asyncio.Lock()  
        self.__doc__ = None  
      
    def __call__(self, func: Callable) -> 'AsyncCachedProperty':  
        self._func = func  
        self.__doc__ = func.__doc__  
        return self  
      
    def __set_name__(self, owner, name):  
        if self._attrname is None:  
            self._attrname = name  
        elif name != self._attrname:  
            raise TypeError("Cannot assign AsyncCachedProperty to different names")  
      
    async def __get__(self, instance, owner=None):  
        if instance is None:  
            return self  
          
        cache = instance.__dict__  
        cache_key = f"_{self._attrname}_cached"  
        cache_time_key = f"_{self._attrname}_timestamp"  
          
        now = time.time()  
          
        async with self._lock:  
            cached_value = cache.get(cache_key)  
            cached_time = cache.get(cache_time_key, 0)  
              
            is_valid = (cached_value is not None and   
                        (self._ttl is None or now - cached_time < self._ttl))  
              
            if is_valid:  
                return cached_value  
              
            result = self._func(instance)  
            if asyncio.iscoroutine(result):  
                result = await result  
              
            cache[cache_key] = result  
            cache[cache_time_key] = now  
            return result  
      
    def expire(self, instance) -> bool:  
        cache_key = f"_{self._attrname}_cached"  
        cache_time_key = f"_{self._attrname}_timestamp"  
          
        deleted = False  
        if cache_key in instance.__dict__:  
            del instance.__dict__[cache_key]  
            deleted = True  
        if cache_time_key in instance.__dict__:  
            del instance.__dict__[cache_time_key]  
        return deleted

def cache_clear_all() -> None:
    with _registry_lock:
        for cache_instance in list(_cache_registry):
            try:
                cache_instance.clear()
            except Exception:
                pass

def get_cache_info() -> list[CacheInfo]:
    with _registry_lock:
        return [cache_instance.info() for cache_instance in list(_cache_registry)]

def get_global_stats() -> dict:
    all_info = get_cache_info()
    total_hits = sum(info.hits for info in all_info)
    total_misses = sum(info.misses for info in all_info)
    total_operations = sum(info.total_operations for info in all_info)
    total_memory = sum(info.memory_used for info in all_info)

    total_requests = total_hits + total_misses  
    
    return {  
        'total_caches': len(all_info),  
        'total_hits': total_hits,  
        'total_misses': total_misses,  
        'total_operations': total_operations,  
        'total_memory_bytes': total_memory,  
        'total_memory_mb': total_memory / 1024 / 1024,  
        'global_hit_rate': total_hits / total_requests if total_requests > 0 else 0,  
        'avg_operations_per_cache': total_operations / len(all_info) if all_info else 0  
    }

async def async_cache_clear_all() -> None:
    with _registry_lock:
        for cache_instance in list(_cache_registry):
            try:
                cache_instance.clear()
            except Exception:
                pass

async def async_get_cache_info() -> list[CacheInfo]:
    with _registry_lock:
        return [cache_instance.info() for cache_instance in list(_cache_registry)]

async def async_get_global_stats() -> dict:
    all_info = await async_get_cache_info()
    total_hits = sum(info.hits for info in all_info)
    total_misses = sum(info.misses for info in all_info)
    total_operations = sum(info.total_operations for info in all_info)
    total_memory = sum(info.memory_used for info in all_info)

    total_requests = total_hits + total_misses  
    
    return {  
        'total_caches': len(all_info),  
        'total_hits': total_hits,  
        'total_misses': total_misses,  
        'total_operations': total_operations,  
        'total_memory_bytes': total_memory,  
        'total_memory_mb': total_memory / 1024 / 1024,  
        'global_hit_rate': total_hits / total_requests if total_requests > 0 else 0,  
        'avg_operations_per_cache': total_operations / len(all_info) if all_info else 0  
    }

def shutdown_all_executors():
    _local_executor.shutdown(wait=False, cancel_futures=True)

async def async_shutdown_all_executors():
    _local_executor.shutdown(wait=False, cancel_futures=True)
