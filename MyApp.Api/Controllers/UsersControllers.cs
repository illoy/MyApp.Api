using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using MyApp.Api.Services;
using MyApp.Data;
using MyApp.Data.Data;

namespace MyApp.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class UsersControllers : ControllerBase
    {
        private readonly AppDbContext _context;
        private readonly KafkaProducer _kafka;
        public UsersControllers(AppDbContext context, KafkaProducer kafka)
        {
            _context = context;
            _kafka = kafka;
        }

        [HttpGet]
        public async Task<IActionResult> GetUsers()
        {
            var users = await _context.Users.ToListAsync();
            return Ok(users);
        }

        [HttpPost]
        public async Task<IActionResult> CreateUser([FromBody] User user)
        {
            if (user == null || string.IsNullOrEmpty(user.Name))
            {
                return BadRequest("Invalid user data.");
            }
            _context.Users.Add(user);

            await _context.SaveChangesAsync();
            await _kafka.SendMessageAsync(user);

            return CreatedAtAction(nameof(GetUsers), new { id = user.Id }, user);
        }
    }
}
